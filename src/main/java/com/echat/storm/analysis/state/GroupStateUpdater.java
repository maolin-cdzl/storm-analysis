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

	private DebugCounter _debug = new DebugCounter();

	@Override
	public void prepare(Map conf,TridentOperationContext context) {
		super.prepare(conf,context);
	}

	@Override
	public void updateState(GroupState state, List<TridentTuple> inputs,TridentCollector collector) {
		_debug.countIn(logger,inputs.size());
		logger.info("updateState, input tuple count: " + inputs.size());

		List<GroupEvent> events = new LinkedList<GroupEvent>();
		for(TridentTuple tuple : inputs) {
			events.add( GroupEvent.fromTuple(tuple) );
		}
		List<Put> gRows = new LinkedList<Put>();
		List<Put> tgRows = new LinkedList<Put>();
		for(GroupEvent ev : events) {
			if( ValueConstant.GROUP_TYPE_TEMP.equals(ev.group_type) ) {
				tgRows.add( ev.toTempRow() );
			} else {
				gRows.add( ev.toRow() );
			}
		}
		if( !gRows.isEmpty() ) {
			HTableInterface table = state.getHTable(HBaseConstant.GROUP_EVENT_TABLE);
			if( table != null ) {
				try {
					Object[] result = new Object[gRows.size()];
					table.batch(gRows,result);
				} catch (InterruptedException e) {
					logger.error("Error performing put group event to HBase.", e);
				} catch (IOException e) {
					logger.error("Error performing put group event to HBase.", e);
				} finally {
					if( table != null ) {
						state.returnHTable(table);
					}
				}
			} else {
				logger.error("Can not get HTable instance,lost " + gRows.size() + " group event");
			}
		}
		if( !tgRows.isEmpty() ) {
			HTableInterface table = state.getHTable(HBaseConstant.TEMP_GROUP_EVENT_TABLE);
			if( table != null ) {
				try {
					Object[] result = new Object[tgRows.size()];
					table.batch(tgRows,result);
				} catch (InterruptedException e) {
					logger.error("Error performing put temp group event to HBase.", e);
				} catch (IOException e) {
					logger.error("Error performing put temp group event to HBase.", e);
				} finally {
					if( table != null ) {
						state.returnHTable(table);
					}
				}
			} else {
				logger.error("Can not get HTable instance,lost " + tgRows.size() + " temp group event");
			}
		}

		List<Values> reports = state.update(events);
		if( reports != null ) {
			_debug.countOut(logger,reports.size());
			for(Values v : reports) {
				collector.emit(v);
			}
		}
		logger.info("updateState done, input tuple count: " + inputs.size());
	}
}


