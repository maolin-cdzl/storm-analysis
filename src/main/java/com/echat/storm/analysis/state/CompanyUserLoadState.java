package com.echat.storm.analysis.state;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

import backtype.storm.tuple.Values;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import org.apache.hadoop.hbase.client.Put;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class CompanyUserLoadState extends BaseState implements IUserLoadReportReceiver {
	private static final Logger logger = LoggerFactory.getLogger(CompanyUserLoadState.class);

	static public class Factory implements StateFactory {
		public Factory() {
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			logger.info("[makeState] partition:{} - {}",partitionIndex,numPartitions);
			return new CompanyUserLoadState();
		}
	}

	private HashMap<String,UserLoadRecorder>			_companys;
	private Gson										_gson;
	private LinkedList<Values>							_reports;
	private LinkedList<Put>								_records;

	public CompanyUserLoadState() {
		_companys = new HashMap<String,UserLoadRecorder>();
		_gson = TopologyConstant.createStdGson();
		_reports = new LinkedList<Values>();
		_records = new LinkedList<Put>();
	}


	public void login(final String company,long timestamp,final String uid) {
		UserLoadRecorder s = getRecorder(company);
		s.login(timestamp,uid);
	}

	public void logout(final String company,long timestamp,final String uid) {
		UserLoadRecorder s = getRecorder(company);
		s.logout(timestamp,uid);
	}

	public void broken(final String company,long timestamp,final String uid) {
		UserLoadRecorder s = getRecorder(company);
		s.broken(timestamp,uid);
	}

	public List<Values> pollReport() {
		if( _reports.isEmpty() ) {
			return null;
		} else {
			List<Values> r = _reports;
			_reports = new LinkedList<Values>();
			return r;
		}
	}

	public List<Put> pollRecord() {
		if( _records.isEmpty() ) {
			return null;
		} else {
			List<Put> r = _records;
			_records = new LinkedList<Put>();
			return r;
		}
	}

	private UserLoadRecorder getRecorder(final String company) {
		UserLoadRecorder inst = _companys.get(company);
		if( inst == null ) {
			inst = new UserLoadRecorder(company,this);
			_companys.put(company,inst);
		}
		return inst;
	}

	@Override
	public void onSecondReport(final String id,long bucket,UserLoadSecond report) {
		/* not report company second statistic, too many.
		_reports.add(TimeBucketReport.makeReport(
						id,
						ValueConstant.REPORT_USER_LOAD_SECOND,
						bucket,
						_gson.toJson(report)));
		*/
	}

	@Override
	public void onMinuteReport(final String id,long bucket,UserLoadHour report) {
		final String content = _gson.toJson(report);
		/* Don't emit when no one subscribe on it
		_reports.add(TimeBucketReport.makeReport(
						id,
						ValueConstant.REPORT_USER_LOAD_HOUR,
						bucket,
						content));
		*/
		Put row = new Put(TimeBucketReport.rowKey(id,bucket));
		row.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_REPORT,content.getBytes());
		_records.add(row);
	}
}



