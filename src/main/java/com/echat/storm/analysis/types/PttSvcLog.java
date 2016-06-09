package com.echat.storm.analysis.types;

import java.util.Date;
import java.text.ParseException;
import org.apache.commons.lang.time.DateUtils;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.ITuple;
import com.echat.storm.analysis.constant.FieldConstant;
import com.echat.storm.analysis.constant.TopologyConstant;

public class PttSvcLog {
	public String server;
	public String datetime;
	public String level;
	public String content;
	public String event;
	public String uid;
	public String gid;
	public String result;
	public String ctx;
	public String ip;
	public String device;
	public String devid;
	public String version;
	public String imsi;
	public String expect_payload;
	public String target;
	public String target_got;
	public String target_dent;
	public String count;
	public String sw;
	public String value;

	// no transaction member
	private transient long timestamp = 0;

	static public Fields getFields() {
		return new Fields(
				FieldConstant.SERVER_FIELD,
				FieldConstant.DATETIME_FIELD,
				FieldConstant.LEVEL_FIELD,
				FieldConstant.CONTENT_FIELD,
				FieldConstant.EVENT_FIELD,
				FieldConstant.UID_FIELD,
				FieldConstant.GID_FIELD,
				FieldConstant.RESULT_FIELD,
				FieldConstant.CTX_FIELD,
				FieldConstant.IP_FIELD,
				FieldConstant.DEVICE_FIELD,
				FieldConstant.DEVICE_ID_FIELD,
				FieldConstant.VERSION_FIELD,
				FieldConstant.IMSI_FIELD,
				FieldConstant.EXPECT_PAYLOAD_FIELD,
				FieldConstant.TARGET_FIELD,
				FieldConstant.TARGET_GOT_FIELD,
				FieldConstant.TARGET_DENT_FIELD,
				FieldConstant.COUNT_FIELD,
				FieldConstant.SW_FIELD,
				FieldConstant.VALUE_FIELD
					);
	}
	
	static public PttSvcLog fromTuple(ITuple tuple) {
		PttSvcLog log = new PttSvcLog();
		log.server = tuple.getString(0);
		log.datetime = tuple.getString(1);
		log.level = tuple.getString(2);
		log.content = tuple.getString(3);
		log.event = tuple.getString(4);
		log.uid = tuple.getString(5);
		log.gid = tuple.getString(6);
		log.result = tuple.getString(7);
		log.ctx = tuple.getString(8);
		log.ip = tuple.getString(9);
		log.device = tuple.getString(10);
		log.devid = tuple.getString(11);
		log.version = tuple.getString(12);
		log.imsi = tuple.getString(13);
		log.expect_payload = tuple.getString(14);
		log.target = tuple.getString(15);
		log.target_got = tuple.getString(16);
		log.target_dent = tuple.getString(17);
		log.count = tuple.getString(18);
		log.sw = tuple.getString(19);
		log.value = tuple.getString(20);
		return log;
	}

	public Values toValues() {
		return new Values(
			server,
			datetime,
			level,
			content,
			event,
			uid,
			gid,
			result,
			ctx,
			ip,
			device,
			devid,
			version,
			imsi,
			expect_payload,
			target,
			target_got,
			target_dent,
			count,
			sw,
			value
				);
	}

	public Date getDate() {
		try {
			return DateUtils.parseDate(datetime,TopologyConstant.STD_INPUT_DATETIME_FORMAT);
		} catch( ParseException e) {
			throw new RuntimeException("Bad datetime format: " + datetime);
		}
	}
	public long getTimeStamp() {
		if( timestamp == 0 ) {
			timestamp = getDate().getTime();
		}
		return timestamp;
	}
}

