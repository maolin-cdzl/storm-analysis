package com.echat.storm.analysis.state;

import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

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

public class GroupState extends BaseState {
	private static final Logger logger = LoggerFactory.getLogger(GroupState.class);

	static public class Factory implements StateFactory {
		private RedisConfig _redisConfig;

		public Factory(RedisConfig redisConf) {
			_redisConfig = redisConf;
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new GroupState(_redisConfig);
		}
	}

	private TimelineUtil<String>						_timeline;
	//private HashMap<String,HashSet<String>		_

	public TimelineUtil<String> getTimeline() {
		return _timeline;
	}
}


