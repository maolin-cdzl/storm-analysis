package com.echat.storm.analysis.state;

import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import backtype.storm.tuple.Values;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;


public class SpeakState extends BaseState {
	private static final Logger logger = LoggerFactory.getLogger(SpeakState.class);

	static public class Factory implements StateFactory {
		private RedisConfig _redisConfig;
		private HBaseConfig _hbaseConfig;

		public Factory(RedisConfig redisConf,HBaseConfig hbaseConf) {
			_redisConfig = redisConf;
			_hbaseConfig = hbaseConf;
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new SpeakState(_redisConfig,_hbaseConfig);
		}
	}

	public SpeakState(RedisConfig rc,HBaseConfig hc) {
		super(rc,hc);

		_timeline = new TimelineUtil<String>(10000);
		_timelineServer = new TimelineUtil<String>(100);
		_serverLastReport = new HashMap<String,Long>();
		_groupJoined = new HashMap<String,GroupRuntimeInfo>();
		_groupLeft = new HashMap<String,GroupRuntimeInfo>();
		_gson = TopologyConstant.createStdGson();
	}
}
