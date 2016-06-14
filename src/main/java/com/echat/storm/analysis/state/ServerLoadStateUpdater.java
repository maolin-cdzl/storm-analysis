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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTableInterface;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class ServerLoadStateUpdater extends BaseStateUpdater<ServerLoadState> {
	private static final Logger logger = LoggerFactory.getLogger(ServerLoadStateUpdater.class);

	private DebugCounter _debug = new DebugCounter();
	private Gson _gson;

	@Override
	public void prepare(Map conf,TridentOperationContext context) {
		super.prepare(conf,context);
		_gson = TopologyConstant.createStdGson();
	}

	@Override
	public void updateState(ServerLoadState state, List<TridentTuple> inputs,TridentCollector collector) {
		_debug.countIn(logger,inputs.size());
		logger.info("updateState with tuples: {}",inputs.size());
		for(TridentTuple tuple : inputs) {
			TimeBucketReport tbr = TimeBucketReport.fromTuple(tuple);
			if( ValueConstant.REPORT_USER_LOAD_SECOND.equals(tbr.type) ) {
				UserLoadSecond report = _gson.fromJson(tbr.report,UserLoadSecond.class);
				if( report != null ) {
					state.reportUserLoadSecond(tbr.entity,tbr.bucket,report);
				} else {
					logger.error("Error parse: " + tbr.report);
				}
			} else if( ValueConstant.REPORT_USER_LOAD_HOUR.equals(tbr.type) ) {
				UserLoadHour report = _gson.fromJson(tbr.report,UserLoadHour.class);
				if( report != null ) {
					state.reportUserLoadHour(tbr.entity,tbr.bucket,report);
				} else {
					logger.error("Error parse: " + tbr.report);
				}
			} else if( ValueConstant.REPORT_GROUP_LOAD_SECOND.equals(tbr.type) ) {
				GroupLoadReport report = _gson.fromJson(tbr.report,GroupLoadReport.class);
				if( report != null ) {
					state.reportGroupLoadSecond(tbr.entity,tbr.bucket,report);
				} else {
					logger.error("Error parse: " + tbr.report);
				}
			} else if( ValueConstant.REPORT_SPEAKING_LOAD_SECOND.equals(tbr.type) ) {
				SpeakLoadSecond report = _gson.fromJson(tbr.report,SpeakLoadSecond.class);
				if( report != null ) {
					state.reportSpeakLoadSecond(tbr.entity,tbr.bucket,report);
				} else {
					logger.error("Error parse: " + tbr.report);
				}
			} else if( ValueConstant.REPORT_SPEAKING_LOAD_HOUR.equals(tbr.type) ) {
				SpeakLoadHour report = _gson.fromJson(tbr.report,SpeakLoadHour.class);
				if( report != null ) {
					state.reportSpeakLoadHour(tbr.entity,tbr.bucket,report);
				} else {
					logger.error("Error parse: " + tbr.report);
				}
			} else {
				logger.warn("Unkown report type: " + tbr.type);
			}
		}
	}
}

