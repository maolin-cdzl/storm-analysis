package com.echat.storm.analysis.state;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class BaseState implements State {
    public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();
	private static final Logger logger = LoggerFactory.getLogger(BaseState.class);

    @Override
    public void beginCommit(Long txid) {
		logger.info("beginCommit txid: " + txid.toString());
    }

    @Override
    public void commit(Long txid) {
		logger.info("commit txid: " + txid.toString());
    }

    public static class Factory implements StateFactory {
        private RedisConfig _redisConfig = null;
		private HBaseConfig _hbaseConfig = null;
		private int _timelineCacheSize = 0;
		private boolean _withGson = false;

		public Factory withJedis(RedisConfig conf) {
			_redisConfig = conf;
			return this;
		}

		public Factory withHBase(HBaseConfig conf) {
			_hbaseConfig = conf;
			return this;
		}

		public Factory withTimeline(int cacheSize) {
			_timelineCacheSize = cacheSize;
			return this;
		}

		public Factory withGson() {
			_withGson = true;
			return this;
		}

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new BaseState(_redisConfig,_hbaseConfig,_timelineCacheSize,_withGson);
        }
    }

    private JedisPool jedisPool;
	private Configuration hbaseConfiguration;
	private Gson gson;
	private LRUHashMap<String,Long> timelineMap;

    public BaseState(RedisConfig redisConf,HBaseConfig hbaseConf,int timelineCacheSize,boolean withGson) {
		if( redisConf != null ) {
			jedisPool = new JedisPool(DEFAULT_POOL_CONFIG,redisConf.host,redisConf.port);
		}
		if( hbaseConf != null ) {
			hbaseConfiguration = HBaseConfiguration.create();
			//TODO:
		}
		if( timelineCacheSize != 0 ) {
			timelineMap = new LRUHashMap<String,Long>(timelineCacheSize);
		}
		if( withGson ) {
			gson = new GsonBuilder().setDateFormat(TopologyConstant.STD_DATETIME_FORMAT).create();
		}
    }

    public Jedis getJedis() {
		if( jedisPool == null ) {
			throw new RuntimeException("Did not setup jedis configure");
		}
        return jedisPool.getResource();
    }

    public void returnJedis(Jedis jedis) {
		if( jedisPool == null ) {
			throw new RuntimeException("Did not setup jedis configure");
		}
        jedis.close();
    }

	public HTable getHTable(final String tableName) {
		if( hbaseConfiguration == null ) {
			throw new RuntimeException("Did not setup hbase configure");
		}

		HTable table = null;
		try {
			table = new HTable(hbaseConfiguration,tableName);
		} catch( IOException e ) {
			logger.error(e.getMessage());
			return null;
		}
		return table;
	}
	public void returnHTable(HTable table) {
		if( hbaseConfiguration == null ) {
			throw new RuntimeException("Did not setup hbase configure");
		}
		try {
			table.close();
		} catch( IOException e ) {
			logger.error(e.getMessage());
		}
	}

	public Gson getGson() {
		if( gson == null ) {
			throw new RuntimeException("Did not setup gson configure");
		}
		return gson;
	}

	public Long getTimeline(String type,String id) {
		if( timelineMap == null ) {
			throw new RuntimeException("Did not setup timeline configure");
		}
		final String key = type + id;
		return timelineMap.get(key);
	}

	public boolean updateTimeline(String type,String id,Long tl) {
		if( timelineMap == null ) {
			throw new RuntimeException("Did not setup timeline configure");
		}
		final String key = type + id;
		Long last = timelineMap.get(key);
		if( last == null || last <= tl ) {
			timelineMap.put(key,tl);
			return true;
		} else {
			return false;
		}
	}

	public boolean isTooOld(String type,String id,Long tl) {
		if( timelineMap == null ) {
			throw new RuntimeException("Did not setup timeline configure");
		}
		final String key = type + id;
		Long last = timelineMap.get(key);
		if( last != null && last > tl ) {
			return true;
		} else {
			return false;
		}
	}

	public boolean timelineBefore(String id,String type1,String type2) {
		if( timelineMap == null ) {
			throw new RuntimeException("Did not setup timeline configure");
		}
		Long t1 = getTimeline(type1,id);
		Long t2 = getTimeline(type2,id);
		if( t2 == null ) {
			return true;
		} else if( t1 == null ) {
			return false;
		} else {
			return t1 < t2;
		}
	}

}


