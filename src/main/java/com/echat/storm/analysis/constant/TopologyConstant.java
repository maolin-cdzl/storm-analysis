package com.echat.storm.analysis.constant;

import com.echat.storm.analysis.types.RedisConfig;

public class TopologyConstant {
	// debug
	public static final boolean DEBUG = true;

	// datetime string format
	public static final String[] INPUT_DATETIME_FORMAT = new String[] { 
		"yyyy-MM-dd HH:mm:ss.SSS",
		"yyyy/MM/dd HH:mm:ss",
		"yyyy-MM-dd HH:mm:ss"
   	};
	public static final String STD_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final String[] STD_INPUT_DATETIME_FORMAT = new String[] { STD_DATETIME_FORMAT };

	// spout and bolts
	public static final String KAFKA_PTTSVC_SPOUT = "pttsvc-spout";

}


