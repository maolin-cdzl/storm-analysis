package com.echat.storm.analysis.state;

import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

import backtype.storm.tuple.Values;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTableInterface;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

class ServerLoadBucket<T> {
	public long			bucket;
	public T			report;
}

public class ServerLoadState extends BaseState {
	private static final Logger logger = LoggerFactory.getLogger(ServerLoadState.class);

	static public class Factory implements StateFactory {
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new ServerLoadState();
		}
	}

	Gson																_gson;
	HashMap<String,TimeBucketSlidingWindow<ServerLoadSecond>>			_secondSliding;
	HashMap<String,ServerLoadBucket<UserLoadHour>>						_serverUserHour;
	HashMap<String,ServerLoadBucket<SpeakLoadHour>>						_serverSpeakHour;

	public ServerLoadState() {
		_gson = TopologyConstant.createStdGson();
		_secondSliding = new HashMap<String,TimeBucketSlidingWindow<ServerLoadSecond>>();
		_serverUserHour = new HashMap<String,ServerLoadBucket<UserLoadHour>>();
		_serverSpeakHour = new HashMap<String,ServerLoadBucket<SpeakLoadHour>>();
	}

	public void reportUserLoadSecond(final String server,long bucket,final UserLoadSecond report) {
		getSecondReport(server,bucket).userLoad.merge(report);
	}

	public void reportUserLoadHour(final String server,long bucket,final UserLoadHour report) {
		logger.info(server + " " + TopologyConstant.formatDatetime(bucket) + " UserLoad minutes report");

		ServerLoadBucket<UserLoadHour> slb = _serverUserHour.get(server);
		if( slb == null ) {
			slb = new ServerLoadBucket<UserLoadHour>();
			slb.bucket = bucket;
			slb.report = report;
			_serverUserHour.put(server,slb);
			return;
		}

		if( slb.bucket == bucket ) {
			slb.report.merge(report);
		} else if( slb.bucket < bucket ) {
			Put row = new Put(TimeBucketReport.rowKey(server,slb.bucket));
			row.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_REPORT,_gson.toJson(slb.report).getBytes());

			HTableInterface table = getHTable(HBaseConstant.SERVER_USER_LOAD_TABLE);
			if( table != null ) {
				try {
					table.put(row);
				} catch (IOException e) {
					logger.error("Error performing put company speak load to HBase.", e);
				} finally {
					returnHTable(table);
				}
			} else {
				logger.error("Can not get HTable instance for table: " + HBaseConstant.SERVER_USER_LOAD_TABLE);
			}

			slb.bucket = bucket;
			slb.report = report;
		} else {
			logger.warn("expired UserLoadSecond of server: " + server + ", current: " + TopologyConstant.formatDatetime(slb.bucket));
		}
	}

	public void reportGroupLoadSecond(final String server,long bucket,final GroupLoadReport report) {
		getSecondReport(server,bucket).groupLoad.merge(report);
	}

	public void reportSpeakLoadSecond(final String server,long bucket,final SpeakLoadSecond report) {
		getSecondReport(server,bucket).speakLoad.merge(report);
	}

	public void reportSpeakLoadHour(final String server,long bucket,final SpeakLoadHour report) {
		logger.info(server + " " + TopologyConstant.formatDatetime(bucket) + " SpeakLoad minutes report");

		ServerLoadBucket<SpeakLoadHour> slb = _serverSpeakHour.get(server);
		if( slb == null ) {
			slb = new ServerLoadBucket<SpeakLoadHour>();
			slb.bucket = bucket;
			slb.report = report;
			_serverSpeakHour.put(server,slb);
			return;
		}

		if( slb.bucket == bucket ) {
			slb.report.merge(report);
		} else if( slb.bucket < bucket ) {
			Put row = new Put(TimeBucketReport.rowKey(server,bucket));
			row.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_REPORT,_gson.toJson(report).getBytes());

			HTableInterface table = getHTable(HBaseConstant.SERVER_SPEAK_LOAD_TABLE);
			if( table != null ) {
				try {
					table.put(row);
				} catch (IOException e) {
					logger.error("Error performing put company speak load to HBase.", e);
				} finally {
					returnHTable(table);
				}
			} else {
				logger.error("Can not get HTable instance for table: " + HBaseConstant.SERVER_USER_LOAD_TABLE);
			}
			slb.bucket = bucket;
			slb.report = report;
		} else {
			logger.warn("expired SpeakLoadSecond of server: " + server + ", current: " + TopologyConstant.formatDatetime(slb.bucket));
		}
	}


	private ServerLoadSecond getSecondReport(final String server,long bucket) {
		TimeBucketSlidingWindow<ServerLoadSecond> sliding = _secondSliding.get(server);
		if( sliding == null ) {
			sliding = new TimeBucketSlidingWindow<ServerLoadSecond>(TopologyConstant.SECOND_MILLIS,TopologyConstant.SECOND_MILLIS * 3, new ITimeBucketSlidingWindowCallback<ServerLoadSecond>() {
				private final String _server;
				{
					_server = server;
				}
				@Override
				public ServerLoadSecond createItem(long bucket) {
					return new ServerLoadSecond(_server,bucket);
				}
				@Override
				public void onSlidingOut(long bucket,ServerLoadSecond val) {
					onSecondReport(_server,bucket,val);
				}
				@Override
				public void onSkip(long bucket) {
					onSecondReportMissing(_server,bucket);
				}
			});

			_secondSliding.put(server,sliding);
		}
		return sliding.get(bucket);
	}

	private void onSecondReportMissing(final String server,long bucket) {
		logger.warn(server + " second report missing: " + TopologyConstant.formatDatetime(bucket));
		ServerLoadSecond report = new ServerLoadSecond(server,bucket);
		// empty reports
		onSecondReport(server,bucket,report);
	}

	private void onSecondReport(final String server,long bucket,ServerLoadSecond report) {
		final String content = _gson.toJson(report);
		Jedis jedis = getJedis();
		if( jedis != null ) {
			jedis.publish("server-load-" + server,content);
			returnJedis(jedis);
		}
	}

}

