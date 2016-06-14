package com.echat.storm.analysis.constant;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.echat.storm.analysis.types.RedisConfig;

public class TopologyConstant {
	// debug
	public static final boolean DEBUG = true;
	public static final long LOG_REPORT_PERIOD = 10000;	// 10 second

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



	static public Gson createStdGson() {
		return new GsonBuilder().setDateFormat(TopologyConstant.STD_DATETIME_FORMAT).create();
	}

	static public String formatDatetime(final java.util.Date date) {
		return DateFormatUtils.format(date,STD_DATETIME_FORMAT);
	}

	static public String formatDatetime(long millis) {
		return DateFormatUtils.format(new java.util.Date(millis),STD_DATETIME_FORMAT);
	}

	static public java.util.Date parseDatetime(final String datetime) {
		try {
			return DateUtils.parseDate(datetime,STD_INPUT_DATETIME_FORMAT);
		} catch( java.text.ParseException e) {
			throw new RuntimeException("Bad datetime format: " + datetime);
		}
	}

	static public java.util.Date widelyParseDatetime(final String datetime) throws java.text.ParseException {
		return DateUtils.parseDate(datetime,INPUT_DATETIME_FORMAT);
	}

	public static final int SECOND_MILLIS = 1000;
	public static final int MINUTE_MILLIS = 1000 * 60;
	public static final int HOUR_MILLIS = 1000 * 60 * 60;
	public static final int HOUR_SECONDS = 3600;

	static public long toSecondBucket(long ts) {
		return (ts / SECOND_MILLIS) * SECOND_MILLIS;
	}

	static public long toMinuteBucket(long ts) {
		return (ts / MINUTE_MILLIS) * MINUTE_MILLIS;
	}

}


