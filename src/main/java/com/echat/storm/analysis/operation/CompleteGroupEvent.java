package com.echat.storm.analysis.operation;

import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;

import redis.clients.jedis.Jedis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import com.echat.storm.analysis.constant.FieldConstant;
import com.echat.storm.analysis.constant.ValueConstant;
import com.echat.storm.analysis.types.RedisConfig;
import com.echat.storm.analysis.utils.LRUHashMap;

public class CompleteGroupEvent extends BaseFunction {
	private static final Logger logger = LoggerFactory.getLogger(CompleteGroupEvent.class);
	static private final int MAX_CACHE_COUNT = 10000;
	static private final long TIMEOUT_MILLIS = 5 * 60 * 1000; // 5 minute

	private class OrganizationInfo {
		public String			company;
		public String			agent;
	}

	private class TimedOrganizationInfo {
		public long					expired;
		public OrganizationInfo		info;
	}

	static public Fields getOutputFields() {
		return new Fields(
				FieldConstant.GROUP_TYPE_FIELD,
				FieldConstant.GROUP_COMPANY_FIELD,
				FieldConstant.GROUP_AGENT_FIELD);
	}

    private Jedis jedis;
	private LRUHashMap<String,TimedOrganizationInfo> cache;
	
	public CompleteGroupEvent(RedisConfig config) {
		jedis = new Jedis(config.host,config.port,config.timeout);
		cache = new LRUHashMap<String,TimedOrganizationInfo>(MAX_CACHE_COUNT);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		final String gid = tuple.getString(0);
		final String type = getGroupType(gid);
		if( type == null ) {
			logger.error("Bad GID format: " + gid);
			return;
		}
		final OrganizationInfo info = search(gid);
		if( info != null && info.company != null ) {
			collector.emit(new Values(type,info.company,info.agent));
		} else {
			logger.warn("Can not found OrganizationInfo for " + gid);
		}
	}

	private String getGroupType(final String gid) {
		try {
			long g = Long.parseLong(gid);
			if( g >= 0x70000000L ) {
				return ValueConstant.GROUP_TYPE_TEMP;
			} else {
				return ValueConstant.GROUP_TYPE_NORMAL;
			}
		} catch( NumberFormatException e ) {
			return null;
		}
	}



	private OrganizationInfo search(final String gid) {
		final long now = System.currentTimeMillis();
		TimedOrganizationInfo ti = cache.get(gid);

		// cache it even if not found.
		if( ti != null ) {
			if( ti.expired >= now ) {
				ti.info = readFromRedis(gid);
				ti.expired = now + TIMEOUT_MILLIS;
			}
		} else {
			ti = new TimedOrganizationInfo();
			ti.expired = now + TIMEOUT_MILLIS;
			ti.info = readFromRedis(gid);

			cache.put(gid,ti);
		}
		return ti.info;
	}

	private OrganizationInfo readFromRedis(String gid) {
		String company = jedis.get("db:group:" + gid + ":company");
		if( company != null ) {
			OrganizationInfo info = new OrganizationInfo();
			info.company = company;
			info.agent = jedis.get("db:company:" + company + ":agent");
			return info;
		} else {
			return null;
		}
	}
}


