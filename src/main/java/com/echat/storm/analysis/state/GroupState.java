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
		private HBaseConfig _hbaseConfig;

		public Factory(RedisConfig redisConf,HBaseConfig hbaseConf) {
			_redisConfig = redisConf;
			_hbaseConfig = hbaseConf;
		}
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new GroupState(_redisConfig,_hbaseConfig);
		}
	}


	private TimelineUtil<>									_userTimeline;
	private TimelineUtil<>									_groupTimeline;
	private LRUHashMap<String,SpeakingRecord.Builder>		_speakingBuilders;
	private TimelineUtil<>									_timelineServer;
	private HashMap<String,Long>							_serverLastReport;
	private HashMap<String,GroupRuntimeInfo>				_groupJoined;
	private HashMap<String,GroupRuntimeInfo>				_groupLeft;
	private Gson											_gson;

	public GroupState(RedisConfig rc,HBaseConfig hc) {
		super(rc,hc);

		_userTimeline = new TimelineUtil<>(10000);
		_groupTimeline = new TimelineUtil<>(10000);
		_speakingBuilders = new LRUHashMap<String,SpeakingRecord.Builder>(10000);
		_timelineServer = new TimelineUtil<>(100);
		_serverLastReport = new HashMap<String,Long>();
		_groupJoined = new HashMap<String,GroupRuntimeInfo>();
		_groupLeft = new HashMap<String,GroupRuntimeInfo>();
		_gson = TopologyConstant.createStdGson();
	}

	public List<Values> update(List<GroupEvent> events) {
		List<Values> reports = null;
		List<SpeakingRecord> speakings = new LinkedList<SpeakingRecord>();
		Jedis jedis = null;
		try {
			jedis = getJedis();
			Pipeline pipe = jedis.pipelined();

			for(GroupEvent ev : events) {
				if( EventConstant.EVENT_JOIN_GROUP.equals(ev.event) ) {
					_timelineServer.update(ev.server,ev.getTimeStamp(),null);
					join(pipe,ev);
				} else if( EventConstant.EVENT_LEAVE_GROUP.equals(ev.event) ) {
					_timelineServer.update(ev.server,ev.getTimeStamp(),null);
					leave(pipe,ev);
				} else if( EventConstant.EVENT_GET_MIC.equals(ev.event) ) {
					SpeakingRecord record = startSpeak(ev);
					if( record != null ) {
						speakings.add(record);
					}
				} else if( EventConstant.EVENT_LOSTMIC_AUTO.equals(ev.event)
						|| EventConstant.EVENT_LOSTMIC_REPLACE.equals(ev.event) ) {
					SpeakingRecord record = stopSpeak(ev);
					if( record != null ) {
						speakings.add(record);
					}
				} else {
					logger.warn("Unkown event: " + ev.event);
				}
			}
			processJoinedGroup(pipe);
			List<MemberCountResponse> ress = processLeftGroup(pipe);
			List<GroupReportResponse> reportResponse = checkReport();
			pipe.sync();
			
			if( ress != null ) {
				processLeftGroupResponse(pipe,ress);
			}
			if( reportResponse != null ) {
				reports = processReportResponse(reportResponse);
			}
			pipe.sync();
		} finally {
			returnJedis(jedis);
		}

		if( !speakings.isEmpty() ) {
			storeSpeakings(speakings);
		}

		return reports;
	}

	private void join(Pipeline pipe,final GroupEvent ev) {
		if( _userTimeline.update(ev.uid,ev.getTimeStamp(),null) ) {
			final String groupId = ev.getGroupFullId();
			if( ValueConstant.GROUP_TYPE_NORMAL.equals(ev.group_type) ) {
				pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,groupID);
				pipe.sadd(RedisConstant.GROUP_PREFIX + groupID + RedisConstant.USER_SUFFIX,ev.uid);

				_groupJoined.put(groupID,new GroupRuntimeInfo(false,ev.server,groupID,ev.group_company));
			} else if( ValueConstant.GROUP_TYPE_TEMP.equals(ev.group_type) ) {
				pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,groupId);
				pipe.sadd(RedisConstant.TEMP_GROUP_PREFIX + groupID + RedisConstant.USER_SUFFIX,ev.uid);
				pipe.expire(RedisConstant.TEMP_GROUP_PREFIX + groupID + RedisConstant.USER_SUFFIX,TopologyConstant.HOUR_SECONDS);

				_groupJoined.put(groupID,new GroupRuntimeInfo(true,ev.server,groupID,ev.group_company));
			} else {
				logger.error("Unknown group type: " + ev.group_type);
			}
		}
	}

	private void leave(Pipeline pipe,final GroupEvent ev) {
		if( _userTimeline.update(ev.uid,ev.getTimeStamp(),null) ) {
			final String groupId = ev.getGroupFullId();
			if( ValueConstant.GROUP_TYPE_NORMAL.equals(ev.group_type) ) {
				pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,"0");
				pipe.srem(RedisConstant.GROUP_PREFIX + groupID + RedisConstant.USER_SUFFIX,ev.uid);

				_groupLeft.put(groupId,new GroupRuntimeInfo(false,ev.server,groupId,ev.group_company));
			} else if( ValueConstant.GROUP_TYPE_TEMP.equals(ev.group_type) ) {
				pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,"0");
				pipe.srem(RedisConstant.TEMP_GROUP_PREFIX + groupID + RedisConstant.USER_SUFFIX,ev.uid);
				pipe.expire(RedisConstant.TEMP_GROUP_PREFIX + groupID + RedisConstant.USER_SUFFIX,TopologyConstant.HOUR_SECONDS);
				
				_groupLeft.put(groupID,new GroupRuntimeInfo(true,ev.server,groupID,ev.group_company));
			} else {
				logger.error("Unknown group type: " + ev.group_type);
			}
		}
	}

	private SpeakingRecord startSpeak(final GroupEvent ev) {
		final String groupId = ev.getGroupFullId();
		if( _groupTimeline.update(groupId,ev.getTimeStamp(),null) ) {
			SpeakingRecord.Builder builder = _speakingBuilders.get(groupId);
			if( builder == null ) {
				builder = SpeakingRecord.createBuilder(ev);
				_speakingBuilders.put(groupId,builder);
			}
			return builder.startSpeak(ev);
		}
		return null;
	}

	private SpeakingRecord stopSpeak(final GroupEvent ev) {
		final String groupId = ev.getGroupFullId();
		if( _groupTimeline.update(groupId,ev.getTimeStamp(),null) ) {
			SpeakingRecord.Builder builder = _speakingBuilders.get(groupId);
			if( builder != null ) {
				return builder.startSpeak(ev);
			}
		}
		return null;
	}

	private void processJoinedGroup(Pipeline pipe) {
		if(! _groupJoined.isEmpty() ) {
			for(GroupRuntimeInfo c : _groupJoined.values()) {
				if( c.isTemp ) {
					pipe.sadd(RedisConstant.ONLINE_TEMP_GROUP_KEY,c.gid);
					pipe.sadd(RedisConstant.SERVER_PREFIX + c.server + RedisConstant.TEMP_GROUP_SUFFIX,c.gid);
					pipe.sadd(RedisConstant.TEMP_GROUP_PREFIX + c.gid + RedisConstant.SERVER_SUFFIX, c.server);
					pipe.expire(RedisConstant.TEMP_GROUP_PREFIX + c.gid + RedisConstant.SERVER_SUFFIX,TopologyConstant.HOUR_SECONDS);
					pipe.sadd(RedisConstant.COMPANY_PREFIX + c.group_company + RedisConstant.TEMP_GROUP_SUFFIX,c.gid);
				} else {
					pipe.sadd(RedisConstant.ONLINE_GROUP_KEY,c.gid);
					pipe.sadd(RedisConstant.SERVER_PREFIX + c.server + RedisConstant.GROUP_SUFFIX,c.gid);
					pipe.sadd(RedisConstant.GROUP_PREFIX + c.gid + RedisConstant.SERVER_SUFFIX, c.server);
					pipe.sadd(RedisConstant.COMPANY_PREFIX + c.group_company + RedisConstant.GROUP_SUFFIX,c.gid);
				}
			}
			_groupJoined.clear();
		}
	}

	private List<MemberCountResponse> processLeftGroup(Pipeline pipe) {
		if( ! _groupLeft.isEmpty() ) {
			List<MemberCountResponse> ress = new LinkedList<MemberCountResponse>();

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
			return ress;
		} else {
			return null;
		}
	}

	private void processLeftGroupResponse(Pipeline pipe,List<MemberCountResponse> ress) {
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
	}

	private List<GroupReportResponse> checkReport() {
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

		if( reportResponse.isEmpty() ) {
			return null;
		} else {
			return reportResponse;
		}
	}

	private List<Values> processReportResponse(List<GroupReportResponse> reportResponse) {
		List<Values> reports = new LinkedList<Values>();
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
		return reports;
	}

	private void storeSpeakings(List<SpeakingRecord> speakings) {
		List<Put> gRows = new LinkedList<Put>();
		List<Put> tgRows = new LinkedList<Put>();
		for(SpeakingRecord s : speakings) {
			if( ValueConstant.GROUP_TYPE_TEMP.equals(s.group_type) ) {
				tgRow.add( s.toTempRow() );
			} else {
				gRows.add( s.toRow() );
			}
		}
		if( !gRows.isEmpty() ) {
			HTableInterface table = getHTable(HBaseConstant.GROUP_SPEAKING_TABLE);
			if( table != null ) {
				try {
					Object[] result = new Object[gRows.size()];
					table.batch(gRows,result);
				} catch (InterruptedException e) {
					logger.error("Error performing put group speaking to HBase.", e);
				} catch (IOException e) {
					logger.error("Error performing put group speaking to HBase.", e);
				} finally {
					if( table != null ) {
						returnHTable(table);
					}
				}
			} else {
				logger.error("Can not get HTable instance,lost " + gRows.size() + " group speaking");
			}
		}
		if( !tgRows.isEmpty() ) {
			HTableInterface table = getHTable(HBaseConstant.TEMP_GROUP_SPEAKING_TABLE);
			if( table != null ) {
				try {
					Object[] result = new Object[tgRows.size()];
					table.batch(tgRows,result);
				} catch (InterruptedException e) {
					logger.error("Error performing put temp group speaking to HBase.", e);
				} catch (IOException e) {
					logger.error("Error performing put temp group speaking to HBase.", e);
				} finally {
					if( table != null ) {
						state.returnHTable(table);
					}
				}
			} else {
				logger.error("Can not get HTable instance,lost " + tgRows.size() + " temp group speaking");
			}
		}
	}
}


