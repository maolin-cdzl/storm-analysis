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
	public Set<String>			logins;
	public Set<String>			logouts;
	public Set<String>			brokens;

	public UserLoadRecord() {
		logins = new HashSet<String>();
		logouts = new HashSet<String>();
		brokens = new HashSet<String>();
	}
}


public class UserLoadRecorder implements ITimeBucketSlidingWindowCallback<UserLoadRecord> {
	private static final long SECOND_SLIDING_WINDOW = 3000;

	private static final Logger logger = LoggerFactory.getLogger(UserLoadRecorder.class);

	public final String									id;
	private IUserLoadReportReceiver						receiver;
	private long										minuteBucket;
	private TimeBucketSlidingWindow<UserLoadRecord>		secondSliding;
	private Set<String>									onlines;
	private TimeoutedSet<String>						timeoutedOnlines;
	private TimeoutedSet<String>						timeoutedLogins;
	private TimeoutedSet<String>						timeoutedLogouts;
	private TimeoutedSet<String>						timeoutedBrokens;
	private LinkedList<UserLoadSecond>					loadHistory;

	public UserLoadRecorder(final String n,IUserLoadReportReceiver r) {
		id = n;
		receiver = r;
		minuteBucket = 0L;
		secondSliding = new TimeBucketSlidingWindow<UserLoadRecord>(TopologyConstant.SECOND_MILLIS,SECOND_SLIDING_WINDOW,this);
		onlines = new HashSet<String>();
		timeoutedOnlines = new TimeoutedSet<String>();
		timeoutedLogins = new TimeoutedSet<String>();
		timeoutedLogouts = new TimeoutedSet<String>();
		timeoutedBrokens = new TimeoutedSet<String>();
		loadHistory = new LinkedList<UserLoadSecond>();
	}

	public void login(long timestamp,final String uid) {
		final long bucket = TopologyConstant.toSecondBucket(timestamp);
		final long timeout = bucket + TopologyConstant.HOUR_MILLIS;

		timeoutedOnlines.put(uid);
		timeoutedLogins.put(uid,timeout);
		UserLoadRecord record = secondSliding.get(bucket);
		if( record != null ) {
			record.logins.add(uid);
		} else {
			logger.warn(uid + " login event timeout: " + TopologyConstant.formatDatetime(timestamp));
		}
	}

	public void logout(long timestamp,final String uid) {
		final long bucket = TopologyConstant.toSecondBucket(timestamp);
		final long timeout = bucket + TopologyConstant.HOUR_MILLIS;

		timeoutedOnlines.put(uid,timeout);
		timeoutedLogouts.put(uid,timeout);
		UserLoadRecord record = secondSliding.get(bucket);
		if( record != null ) {
			record.logouts.add(uid);
		} else {
			logger.warn(uid + " logout event timeout: " + TopologyConstant.formatDatetime(timestamp));
		}
	}

	public void broken(long timestamp,final String uid) {
		final long bucket = TopologyConstant.toSecondBucket(timestamp);
		final long timeout = bucket + TopologyConstant.HOUR_MILLIS;

		timeoutedBrokens.put(uid,timeout);
		UserLoadRecord record = secondSliding.get(bucket);
		if( record != null ) {
			record.brokens.add(uid);
		} else {
			logger.warn(uid + " broken event timeout: " + TopologyConstant.formatDatetime(timestamp));
		}
	}

	@Override
	public UserLoadRecord createItem(long bucket) {
		return new UserLoadRecord();
	}

	@Override
	public void onSlidingOut(long bucket,UserLoadRecord val) {
		logger.info(id + " sliding out: " + bucket);

		// update current online user set
		for(String uid : val.logins) {
			onlines.add(uid);
		}
		for(String uid : val.logouts) {
			onlines.remove(uid);
		}

		UserLoadSecond report = new UserLoadSecond();
		report.onlines = onlines.size();
		report.loginTimes = val.logins.size();
		report.logoutTimes = val.logouts.size();
		report.brokenTimes = val.brokens.size();

		secondReport(bucket,report);
	}

	@Override
	public void onSkip(long bucket) {
		logger.warn(id + " skip " + bucket);
		UserLoadSecond report = loadHistory.peekLast();
		if( report == null ) {
			report = new UserLoadSecond();
		}

		secondReport(bucket,report);
	}

	private void secondReport(long bucket,UserLoadSecond report) {
		loadHistory.add(report);
		if( loadHistory.size() > TopologyConstant.HOUR_SECONDS ) {
			loadHistory.remove();
		}

		receiver.onSecondReport(id,bucket,report);

		if( minuteBucket == 0L ) {
			minuteBucket = TopologyConstant.toMinuteBucket(bucket);
		} else if( bucket >= minuteBucket + TopologyConstant.MINUTE_MILLIS ) {
			minuteBucket = TopologyConstant.toMinuteBucket(bucket);
			minuteReport();
		}
	}

	private void minuteReport() {
		final long start = minuteBucket - TopologyConstant.HOUR_MILLIS;
		UserLoadHour report = new UserLoadHour();

		report.onlines = timeoutedOnlines.values(start).size();
		report.loginUsers = timeoutedLogins.values(start).size();
		report.logoutUsers = timeoutedLogouts.values(start).size();
		report.brokenUsers = timeoutedBrokens.values(start).size();

		for(UserLoadSecond sr : loadHistory) {
			report.loginTimes += sr.loginTimes;
			report.logoutTimes += sr.logoutTimes;
			report.brokenTimes += sr.brokenTimes;
			report.totalOnlineSeconds += sr.onlines;
		}

		receiver.onMinuteReport(id,minuteBucket,report);
	}
}

