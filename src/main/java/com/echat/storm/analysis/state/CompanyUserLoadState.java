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
			return new CompanyUserLoadState();
		}
	}

	private HashMap<String,UserLoadRecorder>			_companys;
	private LinkedList<Values>							reports;
	private Gson										gson;

	public CompanyUserLoadState() {
		super(null,null);
		_companys = new HashMap<String,UserLoadRecorder>();
		reports = new LinkedList<Values>();
		gson = TopologyConstant.createStdGson();
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
		if( reports.isEmpty() ) {
			return null;
		} else {
			List<Values> r = reports;
			reports = new LinkedList<Values>();
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
		reports.add(TimeBucketReport.makeReport(
						id,
						ValueConstant.REPORT_USER_LOAD_SECOND,
						bucket,
						gson.toJson(report)));
		*/
	}

	@Override
	public void onMinuteReport(final String id,long bucket,UserLoadHour report) {
		reports.add(TimeBucketReport.makeReport(
						id,
						ValueConstant.REPORT_USER_LOAD_HOUR,
						bucket,
						gson.toJson(report)));
	}
}



