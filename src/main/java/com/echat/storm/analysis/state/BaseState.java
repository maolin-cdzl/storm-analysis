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
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

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

    static public class Factory implements StateFactory {
        private RedisConfig _redisConfig = null;
		private HBaseConfig _hbaseConfig = null;

		public Factory withJedis(RedisConfig conf) {
			_redisConfig = conf;
			return this;
		}

		public Factory withHBase(HBaseConfig conf) {
			_hbaseConfig = conf;
			return this;
		}

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new BaseState(_redisConfig,_hbaseConfig);
        }
    }

    private JedisPool jedisPool;
	private HBaseConfig hbaseCustomConfig;
	private Configuration hbaseConfiguration;
	private HConnection hbaseConnection;

    public BaseState(RedisConfig redisConf,HBaseConfig hbaseConf) {
		if( redisConf != null ) {
			jedisPool = new JedisPool(DEFAULT_POOL_CONFIG,redisConf.host,redisConf.port);
		}
		if( hbaseConf != null ) {
			hbaseCustomConfig = hbaseConf;
			hbaseConfiguration = HBaseConfiguration.create();
			hbaseConnection = null;
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

	private HConnection createHConnection() throws IOException {
		return HConnectionManager.createConnection(hbaseConfiguration);
	}

	public HTableInterface getHTable(final String tableName) {
		if( hbaseConfiguration == null ) {
			throw new RuntimeException("Did not setup hbase configure");
		}

		HTableInterface table = null;
		try {
			if( hbaseConnection == null ) {
				hbaseConnection = createHConnection();
			}
			table = hbaseConnection.getTable(tableName);
		} catch( IOException e ) {
			if( hbaseConnection != null ) {
				try {
					hbaseConnection.close();
				} catch( IOException ee ) {
				} finally {
					hbaseConnection = null;
				}
			}
			logger.error(e.getMessage());
			return null;
		}
		return table;
	}

	public void returnHTable(HTableInterface table) {
		try {
			if( table != null ) {
				table.close();
			}
		} catch( IOException e ) {
			if( hbaseConnection != null ) {
				try {
					hbaseConnection.close();
				} catch( IOException ee ) {
				} finally {
					hbaseConnection = null;
				}
			}
			logger.error(e.getMessage());
		}
	}
}


