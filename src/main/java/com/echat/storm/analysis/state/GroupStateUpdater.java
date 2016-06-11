package com.echat.storm.analysis.state;

import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.BaseStateUpdater;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class GroupStateUpdater extends BaseStateUpdater<GroupState> {
	private static final Logger logger = LoggerFactory.getLogger(GroupStateUpdater.class);

	private HashSet<String> 
	@Override
	public void prepare(Map conf,TridentOperationContext context) {
		super.prepare(conf,context);
	}

	@Override
	public void updateState(GroupState state, List<TridentTuple> inputs,TridentCollector collector) {
		logger.info("updateState, input tuple count: " + inputs.size());

		TimelineUtil<String> timeline = state.getTimeline();
		Jedis jedis = null;
		try {
			jedis = state.getJedis();
			Pipeline pipe = jedis.pipelined();

			for(TridentTuple tuple : inputs) {
				GroupEvent ev = GroupEvent.fromTuple(tuple);
				if( EventConstant.EVENT_JOIN_GROUP.equals(ev.event) ) {
					join(pipe,timeline,ev);
				} else if( EventConstant.EVENT_LEAVE_GROUP.equals(ev.event) ) {
					leave(pipe,timeline,ev);
				} else {
					logger.warn("Unkown event: " + ev.event);
				}
			}
			pipe.sync();
		} finally {
			state.returnJedis(jedis);
		}
	}

	private void join(Pipeline pipe,TimelineUtil<String> timeline,GroupEvent ev) {
		final String key = ev.gid + ev.uid;
		if( timeline.update(key,ev.getTimeStamp(),ev.event) ) {
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,ev.gid);
			pipe.sadd(RedisConstant.GROUP_PREFIX + ev.gid + RedisConstant.USER_SUFFIX,ev.uid);
		}
	}

	private void leave(GroupEvent ev) {
		final String key = ev.gid + ev.uid;
		if( timeline.update(key,ev.getTimeStamp(),ev.event) ) {
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,"0");
			pipe.srem(RedisConstant.GROUP_PREFIX + ev.gid + RedisConstant.USER_SUFFIX,ev.uid);
		}
	}
}


