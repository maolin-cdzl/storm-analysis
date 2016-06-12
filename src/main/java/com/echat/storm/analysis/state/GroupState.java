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

class GroupRuntimeInfo {
	public final boolean	isTemp;
	public final String		server;
	public final String		gid;
	public final String		company;

	public GroupRuntimeInfo(boolean t,final String s,final String g,final String c) {
		isTemp = t;
		server = s;
		gid = g;
		company = c;
	}
}

class MemberCountResponse {
	public GroupRuntimeInfo			group;			
	public Response<Long>			count;
	public Response<Set<String>>	servers;
}

class GroupReportResponse {
	public String				server;
	public long					bucket;
	public Response<Long>		group_count;
	public Response<Long>		temp_group_count;
}

public class GroupState extends BaseState {
	private static final Logger logger = LoggerFactory.getLogger(GroupState.class);

	static public class Factory implements StateFactory {
		private RedisConfig _redisConfig;

		public Factory(RedisConfig redisConf) {
			_redisConfig = redisConf;
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new GroupState(_redisConfig);
		}
	}


	private TimelineUtil<String>							_timeline;
	private TimelineUtil<String>							_timelineServer;
	private HashMap<String,Long>							_serverLastReport;
	private HashMap<String,GroupRuntimeInfo>				_groupJoined;
	private HashMap<String,GroupRuntimeInfo>				_groupLeft;
	private Gson											_gson;

	public GroupState(RedisConfig rc) {
		super(rc,null);

		_timeline = new TimelineUtil<String>(10000);
		_timelineServer = new TimelineUtil<String>(100);
		_serverLastReport = new HashMap<String,Long>();
		_groupJoined = new HashMap<String,GroupRuntimeInfo>();
		_groupLeft = new HashMap<String,GroupRuntimeInfo>();
		_gson = TopologyConstant.createStdGson();
	}

	public List<Values> update(List<GroupEvent> events) {
		List<Values> reports = new LinkedList<Values>();
		Jedis jedis = null;
		try {
			jedis = getJedis();
			Pipeline pipe = jedis.pipelined();

			for(GroupEvent ev : events) {
				if( EventConstant.EVENT_JOIN_GROUP.equals(ev.event) ) {
					_timelineServer.update(ev.server,ev.getTimeStamp(),ev.event);
					join(pipe,ev);
				} else if( EventConstant.EVENT_LEAVE_GROUP.equals(ev.event) ) {
					_timelineServer.update(ev.server,ev.getTimeStamp(),ev.event);
					leave(pipe,ev);
				} else {
					logger.warn("Unkown event: " + ev.event);
				}
			}
			if(! _groupJoined.isEmpty() ) {
				for(GroupRuntimeInfo c : _groupJoined.values()) {
					if( c.isTemp ) {
						pipe.sadd(RedisConstant.ONLINE_TEMP_GROUP_KEY,c.gid);
						pipe.sadd(RedisConstant.SERVER_PREFIX + c.server + RedisConstant.TEMP_GROUP_SUFFIX,c.gid);
						pipe.sadd(RedisConstant.TEMP_GROUP_PREFIX + c.gid + RedisConstant.SERVER_SUFFIX, c.server);
						pipe.expire(RedisConstant.TEMP_GROUP_PREFIX + c.gid + RedisConstant.SERVER_SUFFIX,TopologyConstant.HOUR_SECONDS);
						pipe.sadd(RedisConfig.COMPANY_PREFIX + c.group_company + RedisConfig.TEMP_GROUP_SUFFIX,c.gid);
					} else {
						pipe.sadd(RedisConstant.ONLINE_GROUP_KEY,c.gid);
						pipe.sadd(RedisConstant.SERVER_PREFIX + c.server + RedisConstant.GROUP_SUFFIX,c.gid);
						pipe.sadd(RedisConstant.GROUP_PREFIX + c.gid + RedisConstant.SERVER_SUFFIX, c.server);
						pipe.sadd(RedisConfig.COMPANY_PREFIX + c.group_company + RedisConfig.GROUP_SUFFIX,c.gid);
					}
				}
				_groupJoined.clear();
			}
			List<MemberCountResponse> ress = new LinkedList<MemberCountResponse>();
			if( ! _groupLeft.isEmpty() ) {

				for(GroupRuntimeInfo c : _groupLeft.values() ) {
					MemberCountResponse mc = new MemberCountResponse();
					mc.group = c;
					if( c.isTemp ) {
						mc.count = pipe.scard(RedisConstant.TEMP_GROUP_PREFIX + c.gid + RedisConstant.USER_SUFFIX);
						mc.servers = pipe.smembers(RedisConstant.TEMP_GROUP_PREFIX + c.gid + RedisConstant.SERVER_SUFFIX);
					} else {
						mc.count = pipe.scard(RedisConstant.GROUP_PREFIX + c.gid + RedisConstant.USER_SUFFIX);
						mc.servers = pipe.smembers(RedisConstant.GROUP_PREFIX + c.gid + RedisConstant.SERVER_SUFFIX);
					}
					ress.add(mc);
				}
				_groupLeft.clear();
			}
			pipe.sync();

			if( !ress.isEmpty() ) {	
				for(MemberCountResponse mc : ress) {
					if( mc.count.get() == 0 ) {
						// group has no members in
						Set<String> servers = mc.servers.get();
						
						if( mc.group.isTemp ) {
							pipe.srem(RedisConstant.ONLINE_TEMP_GROUP_KEY,mc.group.gid);
							pipe.del(RedisConstant.TEMP_GROUP_PREFIX + mc.group.gid + RedisConstant.SERVER_SUFFIX);
							for(String s : servers) {
								pipe.srem(RedisConstant.SERVER_PREFIX + s + RedisConstant.GROUP_SUFFIX,mc.group.gid);
							}
							pipe.srem(RedisConstant.COMPANY_PREFIX + mc.group.company + RedisConstant.TEMP_GROUP_SUFFIX,mc.group.gid);
						} else {
							pipe.srem(RedisConstant.ONLINE_GROUP_KEY,mc.group.gid);
							pipe.del(RedisConstant.GROUP_PREFIX + mc.group.gid + RedisConstant.SERVER_SUFFIX);
							for(String s : servers) {
								pipe.srem(RedisConstant.SERVER_PREFIX + s + RedisConstant.GROUP_SUFFIX,mc.group.gid);
							}
							pipe.srem(RedisConstant.COMPANY_PREFIX + mc.group.company + RedisConstant.GROUP_SUFFIX,mc.group.gid);
						}
					}
				}
				pipe.sync();
			}

			// check report
			List<GroupReportResponse> reportResponse = new LinkedList<GroupReportResponse>();
			for(String server : _timelineServer.keySet()) {
				long ts = TopologyConstant.toMinuteBucket(_timelineServer.get(server).time);
				Long lastReport =  _serverLastReport.get(server);
				if( lastReport == null || lastReport < ts ) {
					lastReport = ts;
					_serverLastReport.put(server,lastReport);
					
					GroupReportResponse res = new GroupReportResponse();
					res.server = server;
					res.bucket = ts;
					res.group_count = pipe.scard(RedisConstant.SERVER_PREFIX + server + RedisConstant.GROUP_SUFFIX);
					res.temp_group_count = pipe.scard(RedisConstant.SERVER_PREFIX + server + RedisConstant.TEMP_GROUP_SUFFIX);
					reportResponse.add(res);
				}
			}
			if( !reportResponse.isEmpty() ) {
				pipe.sync();

				for(GroupReportResponse res : reportResponse) {
					GroupLoadReport report = new GroupLoadReport();
					report.groups = (int)res.group_count.get().longValue();
					report.temp_groups = (int)res.temp_group_count.get().longValue();
					reports.add(TimeBucketReport.makeReport(
							res.server,
							ValueConstant.REPORT_GROUP_LOAD_SECOND,
							res.bucket,
							_gson.toJson(report)));
				}
			}

		} finally {
			returnJedis(jedis);
		}
		return reports;
	}

	private void join(Pipeline pipe,final GroupEvent ev) {
		final String key = ev.gid + ev.uid;
		if( _timeline.update(key,ev.getTimeStamp(),ev.event) ) {
			if( ValueConstant.GROUP_TYPE_NORMAL.equals(ev.group_type) ) {
				pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,ev.gid);
				pipe.sadd(RedisConstant.GROUP_PREFIX + ev.gid + RedisConstant.USER_SUFFIX,ev.uid);

				_groupJoined.put(ev.gid,new GroupRuntimeInfo(false,ev.server,ev.gid,ev.group_company));
			} else if( ValueConstant.GROUP_TYPE_TEMP.equals(ev.group_type) ) {
				final String tgid = getTempGroupID(ev);
				pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,tgid);
				pipe.sadd(RedisConstant.TEMP_GROUP_PREFIX + tgid + RedisConstant.USER_SUFFIX,ev.uid);
				pipe.expire(RedisConstant.TEMP_GROUP_PREFIX + tgid + RedisConstant.USER_SUFFIX,TopologyConstant.HOUR_SECONDS);

				_groupJoined.put(tgid,new GroupRuntimeInfo(true,ev.server,tgid,ev.group_company));
			} else {
				logger.error("Unknown group type: " + ev.group_type);
			}
		}
	}

	private void leave(Pipeline pipe,final GroupEvent ev) {
		final String key = ev.gid + ev.uid;
		if( _timeline.update(key,ev.getTimeStamp(),ev.event) ) {
			if( ValueConstant.GROUP_TYPE_NORMAL.equals(ev.group_type) ) {
				pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,"0");
				pipe.srem(RedisConstant.GROUP_PREFIX + ev.gid + RedisConstant.USER_SUFFIX,ev.uid);

				_groupLeft.put(ev.gid,new GroupRuntimeInfo(false,ev.server,ev.gid,ev.group_company));
			} else if( ValueConstant.GROUP_TYPE_TEMP.equals(ev.group_type) ) {
				final String tgid = getTempGroupID(ev);
				pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,"0");
				pipe.srem(RedisConstant.TEMP_GROUP_PREFIX + tgid + RedisConstant.USER_SUFFIX,ev.uid);
				pipe.expire(RedisConstant.TEMP_GROUP_PREFIX + tgid + RedisConstant.USER_SUFFIX,TopologyConstant.HOUR_SECONDS);
				
				_groupLeft.put(tgid,new GroupRuntimeInfo(true,ev.server,tgid,ev.group_company));
			} else {
				logger.error("Unknown group type: " + ev.group_type);
			}
		}
	}

	private String getTempGroupID(final GroupEvent ev) {
		return ev.server + ":" + ev.gid;
	}

}


