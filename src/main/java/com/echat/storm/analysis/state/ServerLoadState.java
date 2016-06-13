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

public class ServerLoadState extends BaseState {
	private static final Logger logger = LoggerFactory.getLogger(ServerLoadState.class);

	static public class Factory implements StateFactory {
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new ServerLoadState();
		}
	}

	HashMap<String,TimeBucketSlidingWindow<ServerLoadSecond>>			_secondSliding;

	public ServerLoadState() {
	}

	public void reportUserLoadSecond(final String server,final String bucket,final UserLoadSecond report) {
	}

	public void reportUserLoadMinute(final String server,final String bucket,final UserLoadHour report) {
	}

	public void reportGroupLoadSecond(final String server,final String bucket,final GroupLoadReport report) {
	}

	public void reportSpeakLoadSecond(final String server,final String bucket,final SpeakLoadSecond report) {
	}

	public void reportSpeakLoadMinute(final String server,final String bucket,final SpeakLoadHour report) {
	}


	private ServerLoadSecond getSecondReport(final String server,final String bucket) {
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
	}

	private void onSecondReport(final String server,long bucket,ServerLoadSecond report) {
	}

}

