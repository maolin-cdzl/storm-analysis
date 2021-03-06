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

public class ServerSpeakLoadStateUpdater extends BaseStateUpdater<ServerSpeakLoadState> {
	private static final Logger logger = LoggerFactory.getLogger(ServerSpeakLoadStateUpdater.class);

	private DebugCounter _debug = new DebugCounter();

	@Override
	public void prepare(Map conf,TridentOperationContext context) {
		super.prepare(conf,context);
		logger.info("[prepare] partitionIndex:{}",context.getPartitionIndex());
	}

	@Override
	public void updateState(ServerSpeakLoadState state, List<TridentTuple> inputs,TridentCollector collector) {
		_debug.countIn(logger,inputs.size());
		logger.info("updateState with tuples: {}",inputs.size());
		for(TridentTuple tuple : inputs) {
			GroupEvent ev = GroupEvent.fromTuple(tuple);
			if( EventConstant.EVENT_GET_MIC.equals(ev.event) ) {
				state.getMic(ev.server,ev.getTimeStamp(),ev.getGroupFullId(),ev.uid);
			} else if( EventConstant.EVENT_RELEASE_MIC.equals(ev.event) ) {
				state.releaseMic(ev.server,ev.getTimeStamp(),ev.getGroupFullId(),ev.uid);
			} else if( EventConstant.EVENT_LOSTMIC_AUTO.equals(ev.event) ) {
				state.lostMicAuto(ev.server,ev.getTimeStamp(),ev.getGroupFullId(),ev.uid);
			} else if( EventConstant.EVENT_LOSTMIC_REPLACE.equals(ev.event) ) {
				state.lostMicReplace(ev.server,ev.getTimeStamp(),ev.getGroupFullId(),ev.uid);
			} else if( EventConstant.EVENT_DENT_MIC.equals(ev.event) ) {
				state.dentMic(ev.server,ev.getTimeStamp(),ev.getGroupFullId(),ev.uid);
			} else {
				logger.warn("Unknown event: " + ev.event);
			}
		}

		List<Values> reports = state.pollReport();
		if( reports != null ) {
			_debug.countOut(logger,reports.size());
			for(Values v : reports) {
				collector.emit(v);
			}
		}
	}
}


