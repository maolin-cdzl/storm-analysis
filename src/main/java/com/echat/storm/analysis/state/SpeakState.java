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


public class SpeakState extends BaseState {
	private static final Logger logger = LoggerFactory.getLogger(SpeakState.class);

	static public class Factory implements StateFactory {
		private RedisConfig _redisConfig;
		private HBaseConfig _hbaseConfig;

		public Factory(RedisConfig redisConf,HBaseConfig hbaseConf) {
			_redisConfig = redisConf;
			_hbaseConfig = hbaseConf;
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new SpeakState(_redisConfig,_hbaseConfig);
		}
	}

	private HashMap<String,SpeakLoadRecorder>			_serverLoads;
	private HashMap<String,SpeakLoadRecorder>			_companyLoads;
	private Gson										_gson;
	private LinkedList<Values>							_reports;

	public SpeakState(RedisConfig rc,HBaseConfig hc) {
		super(rc,hc);
		
		_serverLoads = new HashMap<String,SpeakLoadRecorder>();
		_companyLoads = new HashMap<String,SpeakLoadRecorder>();
		_gson = TopologyConstant.createStdGson();
		_reports = new LinkedList<Values>();
	}

	public void update(List<GroupEvent> events) {
		for(GroupEvent ev : events) {

		}
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

	private SpeakLoadRecorder getServerRecorder(final String server) {
		SpeakLoadRecorder inst = _serverLoads.get(server);
		if( inst == null ) {
			inst = new SpeakLoadRecorder(server,this);
			_serverLoads.put(server,inst);
		}
		return inst;
	}

	private SpeakLoadRecorder getCompanyRecorder(final String company) {
		SpeakLoadRecorder inst = _serverLoads.get(server);
		if( inst == null ) {
			inst = new SpeakLoadRecorder(server,this);
			_serverLoads.put(server,inst);
		}
		return inst;
	}

	@Override
	public void onSecondReport(final String id,long bucket,SpeakLoadSecond report) {
	}

	@Override
	public void onMinuteReport(final String id,long bucket,SpeakLoadHour report) {
	}
}
