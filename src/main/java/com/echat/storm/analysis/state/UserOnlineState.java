package com.echat.storm.analysis.state;

import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class UserOnlineState extends BaseState {
	private static final Logger logger = LoggerFactory.getLogger(UserOnlineStateUpdater.class);

	public UserOnlineState(RedisConfig redisConf,HBaseConfig hbaseConf) {
		super(redisConf,hbaseConf);
	}

	static public class Factory implements StateFactory {
		private RedisConfig _redisConfig;
		private HBaseConfig _hbaseConfig;

		public Factory(RedisConfig redisConf,HBaseConfig hbaseConf) {
			_redisConfig = redisConf;
			_hbaseConfig = hbaseConf;
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new UserOnlineState(_redisConfig,_hbaseConfig);
		}
	}
}

