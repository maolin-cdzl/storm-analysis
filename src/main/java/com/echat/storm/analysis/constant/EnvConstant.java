package com.echat.storm.analysis.constant;

public class EnvConstant {
	// zookeeper
	public static final String ZOOKEEPER_HOST_LIST = "base001.hdp.echat.com:2181,base002.hdp.echat.com:2181,base003.hdp.echat.com:2181";

	// kafka
	public static final String KAFKA_ID = "pttsvc";
	public static final String KAFKA_TOPIC = "pttsvc-loginfo";
	public static final int KAFKA_TOPIC_PARTITION = 3;

	// storm
	public static final int STORM_MACHINES_NUMBER = 4;
	public static final int STORM_WORKERS_NUMBER = STORM_MACHINES_NUMBER;
}

