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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;


public class ServerSpeakLoadState extends BaseState implements ISpeakLoadReportReceiver {
	private static final Logger logger = LoggerFactory.getLogger(SpeakState.class);

	static public class Factory implements StateFactory {
		public Factory(){
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new SpeakState();
		}
	}

	private HashMap<String,SpeakLoadRecorder>			_serverLoads;
	private Gson										_gson;
	private LinkedList<Values>							_reports;

	public ServerSpeakLoadState() {
		super(null,null);
		
		_serverLoads = new HashMap<String,SpeakLoadRecorder>();
		_gson = TopologyConstant.createStdGson();
		_reports = new LinkedList<Values>();
	}

	public void getMic(final Server,long timestamp,final String gid,final String uid) {
		getRecorder(server).getMic(timestamp,gid,uid);
	}
	public void releaseMic(final Server,long timestamp,final String gid,final String uid) {
		getRecorder(server).releaseMic(timestamp,gid,uid);
	}
	public void lostMicAuto(final Server,long timestamp,final String gid,final String uid) {
		getRecorder(server).lostMicAuto(timestamp,gid,uid);
	}
	public void lostMicReplace(final Server,long timestamp,final String gid,final String uid) {
		getRecorder(server).lostMicReplace(timestamp,gid,uid);
	}
	public void dentMic(long timestamp,final String gid,final String uid) {
		getRecorder(server).dentMic(timestamp,gid,uid);
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

	private SpeakLoadRecorder getRecorder(final String server) {
		SpeakLoadRecorder inst = _serverLoads.get(server);
		if( inst == null ) {
			inst = new SpeakLoadRecorder(server,this);
			_serverLoads.put(server,inst);
		}
		return inst;
	}

	@Override
	public void onSecondReport(final String id,long bucket,SpeakLoadSecond report) {
		_reports.add(TimeBucketReport.makeReport(
					id,
					ValueConstant.REPORT_SPEAKING_LOAD_SECOND,
					bucket,
					_gson.toJson(report)));
	}

	@Override
	public void onMinuteReport(final String id,long bucket,SpeakLoadHour report) {
		_reports.add(TimeBucketReport.makeReport(
					id,
					ValueConstant.REPORT_SPEAKING_LOAD_HOUR,
					bucket,
					_gson.toJson(report)));
	}
}

