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

public class ServerUserLoadState extends BaseState {
	private static final Logger logger = LoggerFactory.getLogger(ServerUserLoadState.class);

	static public class Factory implements StateFactory {
		public Factory() {
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new ServerUserLoadState();
		}
	}

	private HashMap<String,ServerUserLoadRecord>				_servers;

	public ServerUserLoadState() {
		super(null,null);
		_servers = new HashMap<String,ServerUserLoadRecord>();
	}


	public void login(final String server,long timestamp,final String uid) {
		ServerUserLoadRecord s = getServer(server);
		s.login(timestamp,uid);
	}

	public void logout(final String server,long timestamp,final String uid) {
		ServerUserLoadRecord s = getServer(server);
		s.logout(timestamp,uid);
	}

	public void broken(final String server,long timestamp,final String uid) {
		ServerUserLoadRecord s = getServer(server);
		s.broken(timestamp,uid);
	}

	public List<Values> pollReport() {
		List<Values> reports = new LinkedList<Values>();
		for(ServerUserLoadRecord inst : _servers.values()) {
			List<Values> r = inst.pollReport();
			if( r != null ) {
				reports.addAll(r);
			}
		}
		return reports;
	}

	private ServerUserLoadRecord getServer(final String server) {
		ServerUserLoadRecord inst = _servers.get(server);
		if( inst == null ) {
			inst = new ServerUserLoadRecord(server);
			_servers.put(server,inst);
		}
		return inst;
	}
}


