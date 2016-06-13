package com.echat.storm.analysis;

import storm.kafka.ZkHosts;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.OpaqueTridentKafkaSpout;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.operation.*;
import com.echat.storm.analysis.spout.*;
import com.echat.storm.analysis.state.*;

public class AnalysisTopology {
	private static final Logger logger = LoggerFactory.getLogger(AnalysisTopology.class);

	private static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();

		// create kafka spout
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(
				new ZkHosts(EnvConstant.ZOOKEEPER_HOST_LIST), 
				EnvConstant.KAFKA_TOPIC,
				EnvConstant.KAFKA_ID
				);
		spoutConf.scheme = new SchemeAsMultiScheme(new PttLogScheme());
		spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime(); 
		spoutConf.ignoreZkOffsets = false;
		Stream logStream = topology.newStream(
				TopologyConstant.KAFKA_PTTSVC_SPOUT,
				new OpaqueTridentKafkaSpout(spoutConf))
			.partitionBy(new Fields(FieldConstant.SERVER_FIELD))
			.parallelismHint(EnvConstant.KAFKA_TOPIC_PARTITION); 


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

		/*
		// store to hbase
		actionStream.partitionPersist(
				new HBaseStateFactory(new HBaseState.Options()
					.withTableName(HBaseConstant.USER_ACTION_TABLE)
					.withMapper(new PttUserActionHBaseMapper())),
				PttUserActionLog.getFields(),
				new HBaseUpdater()
				);

		// update online informations to redis
		TridentState onlineState = actionStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(OnlineUpdater.getOnlineEvents())
				).partitionPersist(
					new BaseState.Factory(TopologyConstant.REDIS_CONFIG),
					OnlineEvent.getFields(),
					new OnlineUpdater(),
					BrokenEvent.getFields()
				);

		Stream brokenStream = onlineState.newValuesStream();
		brokenStream.partitionPersist(
				new HBaseStateFactory(new HBaseState.Options()
					.withTableName(HBaseConstant.BROKEN_HISTORY_TABLE)
					.withMapper(new BrokenEventHBaseMapper())),
				BrokenEvent.getFields(),
				new HBaseUpdater()
				);

		// update group information to redis
		actionStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(GroupUpdater.getGroupEvents())
			)
			.partitionPersist(
				new BaseState.Factory(TopologyConstant.REDIS_CONFIG),
				GroupEvent.getFields(),
				new GroupUpdater()
			);
		
		// log level count
		Stream loadStream = topology.merge(
			logStream.partitionAggregate(
				new Fields(FieldConstant.SERVER_FIELD,FieldConstant.DATETIME_FIELD,FieldConstant.LEVEL_FIELD),
				new FieldBucketAggregator(FieldConstant.SERVER_FIELD,FieldConstant.DATETIME_FIELD,FieldConstant.LEVEL_FIELD),
				new Fields(FieldConstant.SERVER_FIELD,FieldConstant.BUCKET_FIELD,FieldConstant.LOAD_FIELD)),
			eventStream.partitionAggregate(
				new Fields(FieldConstant.SERVER_FIELD,FieldConstant.DATETIME_FIELD,FieldConstant.EVENT_FIELD),
				new FieldBucketAggregator(FieldConstant.SERVER_FIELD,FieldConstant.DATETIME_FIELD,FieldConstant.EVENT_FIELD),
				new Fields(FieldConstant.SERVER_FIELD,FieldConstant.BUCKET_FIELD,FieldConstant.LOAD_FIELD))
		);


		//log.info("loadStream fields: " + Arrays.toString(loadStream.getOutputFields().toList().toArray()));
		TridentState loadState = loadStream.groupBy(new Fields(FieldConstant.SERVER_FIELD,FieldConstant.BUCKET_FIELD)).persistentAggregate(
				ServerLoadState.nonTransactional(TopologyConstant.REDIS_CONFIG),
				new Fields(FieldConstant.LOAD_FIELD),
				new ServerLoadAggregator(),
				new Fields(FieldConstant.SERVER_LOAD_FIELD)
				);

		onlineStream.partitionPersist(
				new BaseState.Factory(TopologyConstant.REDIS_CONFIG),
				new Fields(FieldConstant.SERVER_FIELD,FieldConstant.DEVICE_FIELD),
				new ServerDevUpdater(),
				new Fields()
				);

		TridentState onlineState = onlineStream.partitionPersist(
				new BaseState.Factory(TopologyConstant.REDIS_CONFIG),
				onlineStream.getOutputFields(),
				new OnlineUpdater(),
				new Fields(FieldConstant.BROKEN_EVENT_FIELD)
				);
		*/
		return topology.build();
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException,InterruptedException {
		if( args == null || args.length < 1 ) {
			System.out.println("need cluster or local args");
			return;
		}

		Config conf = new Config();
		String name = AnalysisTopology.class.getSimpleName();

		if( args[0].equalsIgnoreCase("cluster") ) {
			conf.setNumWorkers(EnvConstant.STORM_WORKERS_NUMBER);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, buildTopology());
		} else if( args[0].equalsIgnoreCase("local") ) {
			conf.setDebug(true);
			System.out.println("Submit Topology");

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf,buildTopology());

			System.out.println("Submit successed");
			Thread.sleep(60000);

			System.out.println("shutdown...");
			cluster.shutdown();
		}

	}
}


