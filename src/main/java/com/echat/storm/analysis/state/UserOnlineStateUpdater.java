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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class UserOnlineStateUpdater extends BaseStateUpdater<UserOnlineState> {
	private static final String TIMELINE_ONLINE = "on";
	private static final String TIMELINE_OFFLINE = "of";
	private static final Logger logger = LoggerFactory.getLogger(UserOnlineStateUpdater.class);

	static private class SessionRecord {
		public final UserOnlineEvent	logout;
		public Response<String>			lastLogin;

		public SessionRecord(UserOnlineEvent ev,Pipeline pipe) {
			logout = ev;
			lastLogin = pipe.get(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGIN_SUFFIX);
		}
	}

	private Gson									_gson = null;
	private TimelineUtil<String>					_timeline = null;
	private HTableInterface							_table = null;
	private HashSet<String>							_devices = null;
	private List<UserOnlineEvent>					_emits = null;
	private List<SessionRecord>						_offlines = null;

	@Override
	public void prepare(Map conf,TridentOperationContext context) {
		super.prepare(conf,context);

		_gson = TopologyConstant.createStdGson();
		_timeline = new TimelineUtil<String>(50000);
		_devices = new HashSet<String>();
		_emits = new LinkedList<UserOnlineEvent>();
		_offlines = new LinkedList<SessionRecord>();
	}

	@Override
	public void updateState(UserOnlineState state, List<TridentTuple> inputs,TridentCollector collector) {
		logger.info("updateState, input tuple count: " + inputs.size());

		HashSet<String> newDevice = new HashSet<String>();

		Jedis jedis = null;
		try {
			jedis = state.getJedis();
			Pipeline pipe = jedis.pipelined();

			for(TridentTuple tuple : inputs) {
				UserActionEvent ev = UserActionEvent.fromTuple(tuple);
				if( ev.device != null && !_devices.contains(ev.device) ) {
					newDevice.add(ev.device);
					_devices.add(ev.device);
				}
				if( EventConstant.EVENT_LOGIN.equals(ev.event) ) {
					processLogin(pipe,ev);
				} else if( EventConstant.EVENT_RELOGIN.equals(ev.event) ) {
					processRelogin(pipe,ev);
				} else if( EventConstant.EVENT_BROKEN.equals(ev.event) ) {
					processBroken(pipe,ev);
				} else if( EventConstant.EVENT_LOGOUT.equals(ev.event) ) {
					processLogout(pipe,ev);
				} else {
					logger.error("Unknown event: " + ev.event);
				}
			}
			pipe.sync();
			
			if( ! newDevice.isEmpty() ) {
				for(String device: newDevice) {
					pipe.sadd(RedisConstant.DEVICE_SET_KEY,device);
				}
				pipe.sync();
			}

			if( !_offlines.isEmpty() ) {
				List<Put> puts = new LinkedList<Put>();
				for(SessionRecord r : _offlines) {
					Put put = processSession(r.lastLogin.get(),r.logout);
					if( put != null ) {
						puts.add(put);
					}
				}
				if( !puts.isEmpty() ) {
					HTableInterface table = state.getHTable(HBaseConstant.USER_SESSION_TABLE);
					if( table != null ) {
						try {
							Object[] result = new Object[puts.size()];
							table.batch(puts,result);
						} catch (InterruptedException e) {
							logger.error("Error performing put sessions to HBase.", e);
						} catch (IOException e) {
							logger.error("Error performing put sessions to HBase.", e);
						} finally {
							if( table != null ) {
								state.returnHTable(table);
							}
						}
					} else {
						logger.error("Can not get HTable instance,lost " + puts.size() + " sessions");
					}
				}
				_offlines.clear();
			}

		} finally {
			state.returnJedis(jedis);
		}

		for(UserOnlineEvent ev : _emits) {
			collector.emit(ev.toValues());
		}
		_emits.clear();

	}

	private HTableInterface getSessionTable(BaseState state) {
		if( _table == null ) {
			_table = state.getHTable(HBaseConstant.USER_SESSION_TABLE);
		}
		return _table;
	}
	private void returnSessionTable(BaseState state) {
		if( _table != null ) {
			state.returnHTable(_table);
			_table = null;
		}
	}

	private void processLogin(Pipeline pipe,UserActionEvent ev) {
		final String onlineKey = TIMELINE_ONLINE + ev.uid;
		final String offlineKey = TIMELINE_OFFLINE + ev.uid;

		if( !_timeline.isExpired(onlineKey,ev.getTimeStamp()) && !_timeline.isExpired(offlineKey,ev.getTimeStamp()) && _timeline.before(onlineKey,offlineKey) ) {
			long offtime = 0;
			final TimelineUtil<String>.Value lastLogout = _timeline.get(offlineKey);
			if( lastLogout != null ) {
				offtime = TimeUnit.MILLISECONDS.toSeconds(ev.getTimeStamp() - lastLogout.time);

				if( EventConstant.EVENT_BROKEN.equals(lastLogout.val) && offtime < 300 ) {
					_emits.add(UserOnlineEvent.fromActionEvent(ev,EventConstant.EVENT_USER_BROKEN,offtime));
				}
			}
			setUserOnline(pipe,UserOnlineEvent.fromActionEvent(ev,EventConstant.EVENT_USER_ONLINE,offtime));
		}

		if( _timeline.update(onlineKey,ev.getTimeStamp(),ev.event) ) {
			final String content = _gson.toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGIN_SUFFIX,content);
		} else {
			logger.warn("Stale events " + ev.event + ",datetime: " + 
					ev.datetime + ",last login datetime: " + 
					TopologyConstant.formatDatetime(_timeline.get(onlineKey).time));
		}
	}

	private void processRelogin(Pipeline pipe,UserActionEvent ev) {
		final String onlineKey = TIMELINE_ONLINE + ev.uid;
		final String offlineKey = TIMELINE_OFFLINE + ev.uid;

		if( !_timeline.isExpired(onlineKey,ev.getTimeStamp()) && !_timeline.isExpired(offlineKey,ev.getTimeStamp()) && _timeline.before(onlineKey,offlineKey) ) {
			long offtime = 0;
			final TimelineUtil<String>.Value lastLogout = _timeline.get(offlineKey);
			if( lastLogout != null ) {
				offtime = TimeUnit.MILLISECONDS.toSeconds(ev.getTimeStamp() - lastLogout.time);
			}
			setUserOnline(pipe,UserOnlineEvent.fromActionEvent(ev,EventConstant.EVENT_USER_ONLINE,offtime));
		}

		if( _timeline.update(onlineKey,ev.getTimeStamp(),ev.event) ) {
			final String content = _gson.toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGIN_SUFFIX,content);
		} else {
			logger.warn("Stale events " + ev.event + ",datetime: " + 
					ev.datetime + ",last login datetime: " + 
					TopologyConstant.formatDatetime(_timeline.get(onlineKey).time));
		}

		_emits.add(UserOnlineEvent.fromActionEvent(ev,EventConstant.EVENT_USER_BROKEN,0));
	}

	private void processBroken(Pipeline pipe,UserActionEvent ev) {
		processLogout(pipe,ev);
	}

	private void processLogout(Pipeline pipe,UserActionEvent ev) {
		final String onlineKey = TIMELINE_ONLINE + ev.uid;
		final String offlineKey = TIMELINE_OFFLINE + ev.uid;

		if( !_timeline.isExpired(onlineKey,ev.getTimeStamp()) && !_timeline.isExpired(offlineKey,ev.getTimeStamp()) && _timeline.before(offlineKey,onlineKey) ) {
			long ontime = 0;
			final TimelineUtil<String>.Value lastLogin = _timeline.get(onlineKey);
			if( lastLogin != null ) {
				ontime = TimeUnit.MILLISECONDS.toSeconds(ev.getTimeStamp() - lastLogin.time);
			}
			setUserOffline(pipe,UserOnlineEvent.fromActionEvent(ev,EventConstant.EVENT_USER_OFFLINE,ontime));
		}

		if( _timeline.update(offlineKey,ev.getTimeStamp(),ev.event) ) {
			final String content = _gson.toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGOUT_SUFFIX,content);
		} else {
			logger.warn("Stale events " + ev.event + ",datetime: " + 
					ev.datetime + ",last logout datetime: " + 
					TopologyConstant.formatDatetime(_timeline.get(offlineKey).time));
		}
	}

	private void setUserOnline(Pipeline pipe,UserOnlineEvent ev) {
		pipe.sadd(RedisConstant.ONLINE_USER_KEY,ev.uid);

		pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.STATE_SUFFIX,RedisConstant.STATE_ONLINE);
		pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.SERVER_SUFFIX,ev.server);

		pipe.sadd(RedisConstant.SERVER_PREFIX + ev.server + RedisConstant.USER_SUFFIX,ev.uid);
		pipe.sadd(RedisConstant.COMPANY_PREFIX + ev.company + RedisConstant.USER_SUFFIX,ev.uid);

		if( ev.device != null ) {
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.DEVICE_SUFFIX,ev.device);
			pipe.sadd(RedisConstant.DEVICE_PREFIX + ev.device + RedisConstant.USER_SUFFIX,ev.uid);
		}

		_emits.add(ev);
	}

	private void setUserOffline(Pipeline pipe,UserOnlineEvent ev) {
		pipe.srem(RedisConstant.ONLINE_USER_KEY,ev.uid);

		pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.STATE_SUFFIX,RedisConstant.STATE_OFFLINE);
		pipe.srem(RedisConstant.SERVER_PREFIX + ev.server + RedisConstant.USER_SUFFIX,ev.uid);
		pipe.srem(RedisConstant.COMPANY_PREFIX + ev.company + RedisConstant.USER_SUFFIX,ev.uid);
		_emits.add(ev);
		_offlines.add(new SessionRecord(ev,pipe));
	}

	private Put processSession(final String loginStr,final UserOnlineEvent logout) {
		if( loginStr == null ) {
			logger.warn("Has no login record for uid=" + logout.uid);
			return null;
		}
		UserOnlineEvent login = _gson.fromJson(loginStr,UserOnlineEvent.class);
		if( login == null ) {
			logger.warn("Can not parse to UserOnlineEvent: " + loginStr);
			return null;
		}

		UserSession session = UserSession.create(login,logout);
		if( session != null ) {
			return session.toRow();
		} else {
			return null;
		}
	}

}

