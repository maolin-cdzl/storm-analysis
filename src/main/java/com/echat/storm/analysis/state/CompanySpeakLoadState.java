package com.echat.storm.analysis.state;

import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import backtype.storm.tuple.Values;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import org.apache.hadoop.hbase.client.Put;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;


public class CompanySpeakLoadState extends BaseState implements ISpeakLoadReportReceiver {
	private static final Logger logger = LoggerFactory.getLogger(CompanySpeakLoadState.class);

	static public class Factory implements StateFactory {
		public Factory(){
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new CompanySpeakLoadState();
		}
	}

	private HashMap<String,SpeakLoadRecorder>			_companyLoads;
	private Gson										_gson;
	private LinkedList<Values>							_reports;
	private LinkedList<Put>								_records;

	public CompanySpeakLoadState() {
		super(null,null);
		
		_companyLoads = new HashMap<String,SpeakLoadRecorder>();
		_gson = TopologyConstant.createStdGson();
		_reports = new LinkedList<Values>();
		_records = new LinkedList<Put>();
	}

	public void getMic(final String company,long timestamp,final String gid,final String uid) {
		getRecorder(company).getMic(timestamp,gid,uid);
	}
	public void releaseMic(final String company,long timestamp,final String gid,final String uid) {
		getRecorder(company).releaseMic(timestamp,gid,uid);
	}
	public void lostMicAuto(final String company,long timestamp,final String gid,final String uid) {
		getRecorder(company).lostMicAuto(timestamp,gid,uid);
	}
	public void lostMicReplace(final String company,long timestamp,final String gid,final String uid) {
		getRecorder(company).lostMicReplace(timestamp,gid,uid);
	}
	public void dentMic(final String company,long timestamp,final String gid,final String uid) {
		getRecorder(company).dentMic(timestamp,gid,uid);
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

	private SpeakLoadRecorder getRecorder(final String company) {
		SpeakLoadRecorder inst = _companyLoads.get(company);
		if( inst == null ) {
			inst = new SpeakLoadRecorder(company,this);
			_companyLoads.put(company,inst);
		}
		return inst;
	}

	@Override
	public void onSecondReport(final String id,long bucket,SpeakLoadSecond report) {
		/*
		_reports.add(TimeBucketReport.makeReport(
					id,
					ValueConstant.REPORT_SPEAKING_LOAD_SECOND,
					bucket,
					_gson.toJson(report)));
		*/
	}

	@Override
	public void onMinuteReport(final String id,long bucket,SpeakLoadHour report) {
		final String content = _gson.toJson(report);
		_reports.add(TimeBucketReport.makeReport(
					id,
					ValueConstant.REPORT_SPEAKING_LOAD_HOUR,
					bucket,
					content));

		Put row = new Put(TimeBucketReport.rowKey(id,bucket));
		row.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_REPORT,content.getBytes());
		_records.add(row);
	}

}


