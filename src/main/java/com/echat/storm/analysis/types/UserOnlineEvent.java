package com.echat.storm.analysis.types;

import java.util.Date;
import java.util.Arrays;
import java.util.Comparator;
import java.text.ParseException;

import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;

public class UserOnlineEvent {
	private static final Logger log = LoggerFactory.getLogger(UserOnlineEvent.class);

	public String server;
	public String datetime;
	public String event;
	public String uid;
	public String company;
	public String agent;
	public Long   interval;
	public String ctx;
	public String ip;
	public String device;
	public String devid;
	public String version;
	public String imsi;
	public String expect_payload;

	// no transaction member
	private transient long timestamp = 0;

	static public Fields getFields() {
		return new Fields(
			FieldConstant.SERVER_FIELD,
			FieldConstant.DATETIME_FIELD,
			FieldConstant.EVENT_FIELD,
			FieldConstant.UID_FIELD,
			FieldConstant.COMPANY_FIELD,
			FieldConstant.AGENT_FIELD,
			FieldConstant.INTERVAL_FIELD,
			FieldConstant.CTX_FIELD,
			FieldConstant.IP_FIELD,
			FieldConstant.DEVICE_FIELD,
			FieldConstant.DEVICE_ID_FIELD,
			FieldConstant.VERSION_FIELD,
			FieldConstant.IMSI_FIELD,
			FieldConstant.EXPECT_PAYLOAD_FIELD
		);
	}

	static public UserOnlineEvent fromTuple(ITuple tuple) {
		UserOnlineEvent ev = new UserOnlineEvent();

		ev.server = tuple.getString(0);
		ev.datetime = tuple.getString(1);
		ev.event = tuple.getString(2);
		ev.uid = tuple.getString(3);
		ev.company = tuple.getString(4);
		ev.agent = tuple.getString(5);
		ev.interval = tuple.getLong(6);
		ev.ctx = tuple.getString(7);
		ev.ip = tuple.getString(8);
		ev.device = tuple.getString(9);
		ev.devid = tuple.getString(10);
		ev.version = tuple.getString(11);
		ev.imsi = tuple.getString(12);
		ev.expect_payload = tuple.getString(13);

		return ev;
	}

	static public UserOnlineEvent fromActionEvent(UserActionEvent act,final String event,long interval) {
		UserOnlineEvent ev = new UserOnlineEvent();
		ev.server = act.server;
		ev.datetime = act.datetime;
		ev.event = event;
		ev.uid = act.uid;
		ev.company = act.company;
		ev.agent = act.agent;
		ev.interval = interval;
		ev.ctx = act.ctx;
		ev.ip = act.ip;
		ev.device = act.device;
		ev.devid = act.devid;
		ev.version = act.version;
		ev.imsi = act.imsi;
		ev.expect_payload = act.expect_payload;
		return ev;
	}

	public Date getDate() {
		return TopologyConstant.parseDatetime(datetime);
	}
	public long getTimeStamp() {
		if( timestamp == 0 ) {
			timestamp = getDate().getTime();
		}
		return timestamp;
	}

	public Values toValues() {
		return new Values(
			server,
			datetime,
			event,
			uid,
			company,
			agent,
			interval,
			ctx,
			ip,
			device,
			devid,
			version,
			imsi,
			expect_payload
		);
	}
}

