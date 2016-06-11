package com.echat.storm.analysis.state;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;

import backtype.storm.tuple.Values;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

class UserLoadRecord {
	public final String			server;
	public Set<String>			logins;
	public Set<String>			logouts;
	public Set<String>			brokens;

	public UserLoadRecord(final String ser) {
		server = ser;
		logins = new HashSet<String>();
		logouts = new HashSet<String>();
		brokens = new HashSet<String>();
	}
}


public class ServerUserLoadRecord implements ITimeBucketSlidingWindowCallback<UserLoadRecord> {
	private static final long SECOND_MILLIS = 1000;
	private static final long MINUTE_MILLIS = 1000 * 60;
	private static final long HOUR_MILLIS = 1000 * 60 * 60;
	private static final long HOUR_SECONDS = 3600;
	private static final long SECOND_SLIDING_WINDOW = 3000;

	private static final Logger logger = LoggerFactory.getLogger(ServerUserLoadRecord.class);

	static public long toSecondBucket(long ts) {
		return (ts / SECOND_MILLIS) * SECOND_MILLIS;
	}

	static public long toMinuteBucket(long ts) {
		return (ts / MINUTE_MILLIS) * MINUTE_MILLIS;
	}

	public final String									server;
	private long										minuteBucket;
	private TimeBucketSlidingWindow<UserLoadRecord>		secondSliding;
	private Set<String>									onlines;
	private TimeoutedSet<String>						timeoutedOnlines;
	private TimeoutedSet<String>						timeoutedLogins;
	private TimeoutedSet<String>						timeoutedLogouts;
	private TimeoutedSet<String>						timeoutedBrokens;
	private LinkedList<ServerUserLoadSecond>			loadHistory;
	private LinkedList<Values>							reports;
	private Gson										gson;

	public ServerUserLoadRecord(final String ser) {
		server = ser;
		minuteBucket = 0L;
		secondSliding = new TimeBucketSlidingWindow<UserLoadRecord>(SECOND_MILLIS,SECOND_SLIDING_WINDOW,this);
		onlines = new HashSet<String>();
		timeoutedOnlines = new TimeoutedSet<String>();
		timeoutedLogins = new TimeoutedSet<String>();
		timeoutedLogouts = new TimeoutedSet<String>();
		timeoutedBrokens = new TimeoutedSet<String>();
		loadHistory = new LinkedList<ServerUserLoadSecond>();
		reports = new LinkedList<Values>();
		gson = TopologyConstant.createStdGson();
	}

	public void login(long timestamp,final String uid) {
		final long bucket = toSecondBucket(timestamp);
		final long timeout = bucket + HOUR_MILLIS;

		timeoutedOnlines.put(uid);
		timeoutedLogins.put(uid,timeout);
		UserLoadRecord record = secondSliding.get(bucket);
		record.logins.add(uid);
	}

	public void logout(long timestamp,final String uid) {
		final long bucket = toSecondBucket(timestamp);
		final long timeout = bucket + HOUR_MILLIS;

		timeoutedOnlines.put(uid,timeout);
		timeoutedLogouts.put(uid,timeout);
		UserLoadRecord record = secondSliding.get(bucket);
		record.logouts.add(uid);
	}

	public void broken(long timestamp,final String uid) {
		final long bucket = toSecondBucket(timestamp);
		final long timeout = bucket + HOUR_MILLIS;

		timeoutedBrokens.put(uid,timeout);
		UserLoadRecord record = secondSliding.get(bucket);
		record.brokens.add(uid);
	}

	public List<Values> pollReport() {
		if( ! reports.isEmpty() ) {
			LinkedList<Values> r = reports;
			reports = new LinkedList<Values>();
			return r;
		} else {
			return null;
		}
	}

	@Override
	public UserLoadRecord createItem(long bucket) {
		return new UserLoadRecord(server);
	}

	@Override
	public void onSlidingOut(long bucket,UserLoadRecord val) {
		logger.info(server + " sliding out: " + bucket);

		// update current online user set
		for(String uid : val.logins) {
			onlines.add(uid);
		}
		for(String uid : val.logouts) {
			onlines.remove(uid);
		}

		ServerUserLoadSecond report = new ServerUserLoadSecond();
		report.onlines = onlines.size();
		report.loginTimes = val.logins.size();
		report.logoutTimes = val.logouts.size();
		report.brokenTimes = val.brokens.size();

		secondReport(bucket,report);
	}

	@Override
	public void onSkip(long bucket) {
		logger.warn(server + " skip " + bucket);
		ServerUserLoadSecond report = new ServerUserLoadSecond();

		secondReport(bucket,report);
	}

	private void secondReport(long bucket,ServerUserLoadSecond report) {
		reports.add(
				ServerLoadReport.makeReport(
					server,
					EventConstant.REPORT_USER_LOAD_SECOND,
					bucket,
					gson.toJson(report)));

		loadHistory.add(report);
		if( loadHistory.size() > HOUR_SECONDS ) {
			loadHistory.remove();
		}

		if( minuteBucket == 0L ) {
			minuteBucket = toMinuteBucket(bucket);
		} else if( bucket >= minuteBucket + MINUTE_MILLIS ) {
			minuteBucket += MINUTE_MILLIS;

			minuteReport();
		}
	}

	private void minuteReport() {
		final long start = minuteBucket - HOUR_MILLIS;
		ServerUserLoadHour report = new ServerUserLoadHour();

		report.onlines = timeoutedOnlines.values(start).size();
		report.loginUsers = timeoutedLogins.values(start).size();
		report.logoutUsers = timeoutedLogouts.values(start).size();
		report.brokenUsers = timeoutedBrokens.values(start).size();

		for(ServerUserLoadSecond sr : loadHistory) {
			report.loginTimes += sr.loginTimes;
			report.logoutTimes += sr.logoutTimes;
			report.brokenTimes += sr.brokenTimes;
			report.totalOnlineSeconds += sr.onlines;
		}

		reports.add(
				ServerLoadReport.makeReport(
					server,
					EventConstant.REPORT_USER_LOAD_HOUR,
					minuteBucket,
					gson.toJson(report)));
	}
}

