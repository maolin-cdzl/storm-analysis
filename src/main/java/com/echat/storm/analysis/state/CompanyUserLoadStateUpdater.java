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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class CompanyUserLoadStateUpdater extends BaseStateUpdater<CompanyUserLoadState> {
	private static final Logger logger = LoggerFactory.getLogger(CompanyUserLoadStateUpdater.class);

	@Override
	public void prepare(Map conf,TridentOperationContext context) {
	}

	@Override
	public void updateState(CompanyUserLoadState state, List<TridentTuple> inputs,TridentCollector collector) {
		for(TridentTuple tuple : inputs) {
			UserOnlineEvent ev = UserOnlineEvent.fromTuple(tuple);
			if( EventConstant.EVENT_USER_ONLINE.equals(ev.event) ) {
				state.login(ev.company,ev.getTimeStamp(),ev.uid);
			} else if( EventConstant.EVENT_USER_OFFLINE.equals(ev.event) ) {
				state.logout(ev.company,ev.getTimeStamp(),ev.uid);
			} else if( EventConstant.EVENT_USER_BROKEN.equals(ev.event) ) {
				state.broken(ev.company,ev.getTimeStamp(),ev.uid);
			} else {
				logger.warn("Unknown event: " + ev.event);
			}
		}

		List<Values> reports = state.pollReport();
		if( reports != null ) {
			for(Values v : reports) {
				collector.emit(v);
			}
		}
	}
}


