package com.echat.storm.analysis.state;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.BaseStateUpdater;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class UserActionHBaseUpdater extends BaseStateUpdater<BaseState> {
	private static final Logger logger = LoggerFactory.getLogger(UserActionHBaseUpdater.class);

	private long _count = 0;
	private long _lastLog = 0;

	@Override
	public void updateState(BaseState state, List<TridentTuple> inputs,TridentCollector collector) {
		if( TopologyConstant.DEBUG ) {
			final long now = System.currentTimeMillis();
			_count += inputs.size();
			if( _lastLog == 0 ) {
				_lastLog = now;
			} else if( now - _lastLog >= TopologyConstant.LOG_REPORT_PERIOD ) {
				logger.info("Process " + _count + " in millis " + (now - _lastLog));
				_lastLog = now;
				_count = 0;
			}
		}
		List<Put> puts = new ArrayList<Put>();

		for(TridentTuple tuple : inputs) {
			UserActionEvent ev = UserActionEvent.fromTuple(tuple);
			puts.add(ev.toRow());
		}
		HTableInterface table = state.getHTable(HBaseConstant.USER_ACTION_TABLE);
		if( table == null ) {
			logger.error("Can not get htable instance");
			return;
		}
		Object[] result = new Object[puts.size()];
        try {
            table.batch(puts, result);
        } catch (InterruptedException e) {
            logger.error("Error performing a put to HBase.", e);
        } catch (IOException e) {
            logger.error("Error performing a put to HBase.", e);
        } finally {
			if( table != null ) {
				state.returnHTable(table);
			}
		}
	}
}

