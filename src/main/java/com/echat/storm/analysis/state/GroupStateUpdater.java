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

	@Override
	public void prepare(Map conf,TridentOperationContext context) {
		super.prepare(conf,context);
	}

	@Override
	public void updateState(GroupState state, List<TridentTuple> inputs,TridentCollector collector) {
		logger.info("updateState, input tuple count: " + inputs.size());

		List<GroupEvent> events = new LinkedList<GroupEvent>();
		for(TridentTuple tuple : inputs) {
			events.add( GroupEvent.fromTuple(tuple) );
		}
		List<Values> reports = state.update(events);
		if( reports != null ) {
			for(Values v : reports) {
				collector.emit(v);
			}
		}
	}
}


