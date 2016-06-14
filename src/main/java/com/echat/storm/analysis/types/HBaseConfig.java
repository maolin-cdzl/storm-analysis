package com.echat.storm.analysis.types;

import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.echat.storm.analysis.constant.EnvConstant;

public class HBaseConfig implements Serializable {
	public int poolSize = 10;

	public static Configuration create() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",EnvConstant.ZOOKEEPER_HOST_LIST);
		conf.set("hbase.zookeeper.property.clientPort",Integer.toString(EnvConstant.ZOOKEEPER_PORT));
		return conf;
	}
}


