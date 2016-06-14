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
		logger.info(this.getClass().getName() + " beginCommit txid: " + txid.toString());
    }

    @Override
    public void commit(Long txid) {
		logger.info(this.getClass().getName() + " commit txid: " + txid.toString());
    }

    static public class Factory implements StateFactory {
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return new BaseState();
        }
    }

    private JedisPool jedisPool;
	private Configuration hbaseConfiguration;
	private HConnection hbaseConnection;

    public BaseState() {
		jedisPool = null;
		hbaseConfiguration = HBaseConfig.create();
		hbaseConnection = null;
    }

    public Jedis getJedis() {
		if( jedisPool == null ) {
			jedisPool = new JedisPool(DEFAULT_POOL_CONFIG,RedisConfig.HOST,RedisConfig.PORT);
		}
        return jedisPool.getResource();
    }

    public void returnJedis(Jedis jedis) {
        jedis.close();
    }

	private HConnection createHConnection() throws IOException {
		return HConnectionManager.createConnection(hbaseConfiguration);
	}

	public HTableInterface getHTable(final String tableName) {
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
			logger.error("when create HTable: ",e);
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
			logger.error("when close HTable: ",e);
		}
	}
}


