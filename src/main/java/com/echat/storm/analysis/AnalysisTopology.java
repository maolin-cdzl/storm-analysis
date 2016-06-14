package com.echat.storm.analysis;

import storm.kafka.ZkHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TransactionalTridentKafkaSpout;

import storm.trident.TridentTopology;
import storm.trident.TridentState;
import storm.trident.Stream;
import storm.trident.fluent.GroupedStream;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.generated.StormTopology;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor; 
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin; 
import org.apache.hadoop.hbase.MasterNotRunningException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.operation.*;
import com.echat.storm.analysis.spout.*;
import com.echat.storm.analysis.state.*;

public class AnalysisTopology {
	private static final Logger logger = LoggerFactory.getLogger(AnalysisTopology.class);

	private static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();

		Stream logStream = topology.newStream(
				TopologyConstant.KAFKA_PTTSVC_SPOUT,
				createKafkaSpout())
			.partitionBy(new Fields(FieldConstant.SERVER_FIELD));
			//.parallelismHint(EnvConstant.KAFKA_TOPIC_PARTITION); 


		// setup user action stream
		Stream actionStream = logStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new FieldFilter(FieldConstant.EVENT_FIELD))
			.partitionBy(new Fields(FieldConstant.UID_FIELD))
			.each(
				CompleteOrganizationByUID.getInputFields(),
				new CompleteOrganizationByUID(),
				CompleteOrganizationByUID.getOutputFields())
			.project(UserActionEvent.getFields());
		
		// persist user action event to hbase
		actionStream.partitionPersist(
				new BaseState.Factory(),
				UserActionEvent.getFields(),
				new UserActionHBaseUpdater());

		Stream onlineStream = actionStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(EventConstant.getOnlineEvents()))
			.partitionPersist(
				new UserOnlineState.Factory(),
				UserActionEvent.getFields(),
				new UserOnlineStateUpdater(),
				UserOnlineEvent.getFields())
			.newValuesStream();

		Stream serverUserLoadStream = onlineStream.partitionBy(new Fields(FieldConstant.UID_FIELD))
			.partitionPersist(
				new ServerUserLoadState.Factory(),
				UserOnlineEvent.getFields(),
				new ServerUserLoadStateUpdater(),
				TimeBucketReport.getFields())
			.newValuesStream();

		Stream companyUserLoadStream = onlineStream.partitionBy(new Fields(FieldConstant.COMPANY_FIELD))
			.partitionPersist(
				new CompanyUserLoadState.Factory(),
				UserOnlineEvent.getFields(),
				new CompanyUserLoadStateUpdater(),
				TimeBucketReport.getFields())
			.newValuesStream();

		Stream groupStream = actionStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(EventConstant.getGroupEvents()))
			.partitionBy(
				new Fields(FieldConstant.GID_FIELD))
			.each(
				CompleteGroupEvent.getInputFields(),
				new CompleteGroupEvent(),
				CompleteGroupEvent.getOutputFields())
			.project(GroupEvent.getFields());
		
		Stream serverGroupLoadStream = groupStream.partitionPersist(
				new GroupState.Factory(),
				GroupEvent.getFields(),
				new GroupStateUpdater(),
				TimeBucketReport.getFields())
			.newValuesStream();

		Stream speakEventStream = groupStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(EventConstant.getGroupSpeakEvents()));

		Stream serverSpeakLoadStream = speakEventStream.partitionPersist(
				new ServerSpeakLoadState.Factory(),
				GroupEvent.getFields(),
				new ServerSpeakLoadStateUpdater(),
				TimeBucketReport.getFields())
			.newValuesStream();

		Stream companySpeakLoadStream = speakEventStream
			.partitionBy(new Fields(FieldConstant.GROUP_COMPANY_FIELD))
			.partitionPersist(
					new CompanySpeakLoadState.Factory(),
					GroupEvent.getFields(),
					new CompanySpeakLoadStateUpdater(),
					TimeBucketReport.getFields())
			.newValuesStream();

		topology
			.merge(serverUserLoadStream,serverGroupLoadStream,serverSpeakLoadStream)
			.global()
			.partitionPersist(
					new ServerLoadState.Factory(),
					TimeBucketReport.getFields(),
					new ServerLoadStateUpdater());

		return topology.build();
	}

	private static TransactionalTridentKafkaSpout createKafkaSpout() {
		/* storm-core spout
		SpoutConfig spoutConfig = new SpoutConfig(
				new ZkHosts(EnvConstant.ZOOKEEPER_HOST_PORT_LIST),
			   	EnvConstant.KAFKA_TOPIC,
				EnvConstant.STORM_KAFKA_ZK_PARENT,
				EnvConstant.STORM_KAFKA_ID);
		*/

		// create kafka spout
		TridentKafkaConfig spoutConfig = new TridentKafkaConfig(
				new ZkHosts(EnvConstant.ZOOKEEPER_HOST_PORT_LIST), 
				EnvConstant.KAFKA_TOPIC,
				EnvConstant.STORM_KAFKA_ID
				);
		
		spoutConfig.scheme = new SchemeAsMultiScheme(new PttLogScheme());
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime(); 
		spoutConfig.ignoreZkOffsets = false;
		//return new KafkaSpout(spoutConfig);
		//return new OpaqueTridentKafkaSpout(spoutConfig);
		return new TransactionalTridentKafkaSpout(spoutConfig);
	}

	private static void createBasicTable(HBaseAdmin admin,final String tableName) throws IOException {
		try {
			if( ! admin.tableExists(tableName) ) {
				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
				tableDesc.addFamily(new HColumnDescriptor(HBaseConstant.COLUMN_FAMILY_LOG));
				admin.createTable(tableDesc);
			}
		} catch( IOException e ) {
			logger.error("Can not create htable: " + tableName,e);
			throw e;
		}
	}

	private static void createHTables() throws IOException,MasterNotRunningException  {
		Configuration conf = HBaseConfig.create();
		HBaseAdmin admin = null;
		
		try {
			admin = new HBaseAdmin(conf);
			createBasicTable(admin,HBaseConstant.USER_ACTION_TABLE);
			createBasicTable(admin,HBaseConstant.USER_SESSION_TABLE);
			createBasicTable(admin,HBaseConstant.BROKEN_HISTORY_TABLE);
			createBasicTable(admin,HBaseConstant.GROUP_EVENT_TABLE);
			createBasicTable(admin,HBaseConstant.TEMP_GROUP_EVENT_TABLE);
			createBasicTable(admin,HBaseConstant.GROUP_SPEAKING_TABLE);
			createBasicTable(admin,HBaseConstant.TEMP_GROUP_SPEAKING_TABLE);
			createBasicTable(admin,HBaseConstant.SERVER_USER_LOAD_TABLE);
			createBasicTable(admin,HBaseConstant.COMPANY_USER_LOAD_TABLE);
			createBasicTable(admin,HBaseConstant.SERVER_SPEAK_LOAD_TABLE);
			createBasicTable(admin,HBaseConstant.COMPANY_SPEAK_LOAD_TABLE);
		} catch( MasterNotRunningException e ) {
			logger.error("HBase master not running",e);
			throw e;
		} finally {
			if( admin != null ) {
				admin.close();
			}
		}
	}

	public static void main(String[] args) throws IOException,AlreadyAliveException, InvalidTopologyException, AuthorizationException,InterruptedException {
		if( args == null || args.length < 1 ) {
			System.out.println("need cluster or local args");
			return;
		}

		createHTables();

		Config conf = new Config();
		String name = AnalysisTopology.class.getSimpleName();

		if( args[0].equalsIgnoreCase("cluster") ) {
			conf.setNumWorkers(EnvConstant.STORM_WORKERS_NUMBER);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, buildTopology());
		} else if( args[0].equalsIgnoreCase("local") ) {
			//conf.setDebug(true);
			System.out.println("Submit Topology");

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf,buildTopology());

			System.out.println("Submit successed");
			Thread.sleep(600000); // 10 minutes

			System.out.println("shutdown...");
			cluster.shutdown();
		}

	}

}


