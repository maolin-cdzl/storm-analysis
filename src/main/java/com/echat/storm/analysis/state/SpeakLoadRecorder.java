package com.echat.storm.analysis.state;

import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import backtype.storm.tuple.Values;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

class SpeakLoadRecord {
	public int					getMic;
	public int					lostMicAuto;
	public int					lostMicReplace;
	public int					dent;
}

public class SpeakLoadRecorder implements ITimeBucketSlidingWindowCallback<SpeakLoadRecord> {
	private static final long SECOND_SLIDING_WINDOW = 3000;
	private static final long MAX_SPEAKING_MILLIS = TopologyConstant.MINUTE_MILLIS * 5;

	private static final Logger logger = LoggerFactory.getLogger(SpeakLoadRecorder.class);

	public final String									id;
	private ISpeakLoadReportReceiver					receiver;
	private long										minuteBucket;
	private TimeBucketSlidingWindow<SpeakLoadRecord>	secondSliding;
	private TimeoutedSet<String>						speakings;
	private TimeoutedSet<String>						timeoutedGroups;
	private TimeoutedSet<String>						timeoutedUsers;
	private TimeoutedSet<String>						timeoutedLostAutoUsers;
	private TimeoutedSet<String>						timeoutedLostAutoGroups;
	private TimeoutedSet<String>						timeoutedLostReplaceUsers;
	private TimeoutedSet<String>						timeoutedLostReplaceGroups;
	private TimeoutedSet<String>						timeoutedDentUsers;
	private TimeoutedSet<String>						timeoutedDentGroups;
	private LinkedList<SpeakLoadSecond>					loadHistory;


	public SpeakLoadRecorder(final String n,ISpeakLoadReportReceiver r) {
		id = n;
		receiver = r;
		minuteBucket = 0;
		secondSliding = new TimeBucketSlidingWindow<SpeakLoadRecord>(TopologyConstant.SECOND_MILLIS,SECOND_SLIDING_WINDOW,this);
		speakings = new TimeoutedSet<String>();
		timeoutedGroups = new TimeoutedSet<String>();
		timeoutedUsers = new TimeoutedSet<String>();
		timeoutedLostAutoUsers = new TimeoutedSet<String>();
		timeoutedLostAutoGroups = new TimeoutedSet<String>();
		timeoutedLostReplaceUsers = new TimeoutedSet<String>();
		timeoutedLostReplaceGroups = new TimeoutedSet<String>();
		timeoutedDentUsers = new TimeoutedSet<String>();
		timeoutedDentGroups = new TimeoutedSet<String>();
		loadHistory = new LinkedList<SpeakLoadSecond>();
	}

	public void getMic(long timestamp,final String gid,final String uid) {
		final long bucket = TopologyConstant.toSecondBucket(timestamp);
		final long timeout = bucket + TopologyConstant.HOUR_MILLIS;

		speakings.put(uid,bucket + MAX_SPEAKING_MILLIS);
		timeoutedGroups.put(gid,timeout);
		timeoutedUsers.put(uid,timeout);
		SpeakLoadRecord record = secondSliding.get(bucket);
		if( record != null ) {
			record.getMic += 1;
		} else {
			logger.warn("gid=" + gid + ",uid=" + uid + ", getMic event timeouted: " + TopologyConstant.formatDatetime(timestamp));
		}
	}

	public void releaseMic(long timestamp,final String gid,final String uid) {
		final long bucket = TopologyConstant.toSecondBucket(timestamp);
		final long timeout = bucket + TopologyConstant.HOUR_MILLIS;

		speakings.remove(uid);
		secondSliding.get(bucket);
	}

	public void lostMicAuto(long timestamp,final String gid,final String uid) {
		final long bucket = TopologyConstant.toSecondBucket(timestamp);
		final long timeout = bucket + TopologyConstant.HOUR_MILLIS;

		timeoutedLostAutoUsers.put(uid,timeout);
		timeoutedLostAutoGroups.put(gid,timeout);
		speakings.remove(uid);
		SpeakLoadRecord record = secondSliding.get(bucket);
		if( record != null ) {
			record.lostMicAuto += 1;
		} else {
			logger.warn("gid=" + gid + ",uid=" + uid + ", lostMicAuto event timeouted: " + TopologyConstant.formatDatetime(timestamp));
		}
	}

	public void lostMicReplace(long timestamp,final String gid,final String uid) {
		final long bucket = TopologyConstant.toSecondBucket(timestamp);
		final long timeout = bucket + TopologyConstant.HOUR_MILLIS;

		timeoutedLostReplaceUsers.put(uid,timeout);
		timeoutedLostReplaceGroups.put(gid,timeout);
		speakings.remove(uid);
		SpeakLoadRecord record = secondSliding.get(bucket);
		if( record != null ) {
			record.lostMicReplace += 1;
		} else {
			logger.warn("gid=" + gid + ",uid=" + uid + ", lostMicReplac event timeouted: " + TopologyConstant.formatDatetime(timestamp));
		}
	}

	public void dentMic(long timestamp,final String gid,final String uid) {
		final long bucket = TopologyConstant.toSecondBucket(timestamp);
		final long timeout = bucket + TopologyConstant.HOUR_MILLIS;

		timeoutedDentUsers.put(uid,timeout);
		timeoutedDentGroups.put(gid,timeout);
		secondSliding.get(bucket).dent += 1;
		SpeakLoadRecord record = secondSliding.get(bucket);
		if( record != null ) {
		} else {
			logger.warn("gid=" + gid + ",uid=" + uid + ", getMic event timeouted: " + TopologyConstant.formatDatetime(timestamp));
		}
	}

	@Override
	public SpeakLoadRecord createItem(long bucket) {
		return new SpeakLoadRecord();
	}
	@Override
	public void onSlidingOut(long bucket,SpeakLoadRecord val) {
		logger.info(id + " sliding out: " + TopologyConstant.formatDatetime(bucket));

		SpeakLoadSecond report = new SpeakLoadSecond();
		report.speakings = speakings.values().size();
		report.getTimes = val.getMic;
		report.lostAutoTimes = val.lostMicAuto;
		report.lostReplaceTimes = val.lostMicReplace;
		report.dentTimes = val.dent;

		secondReport(bucket,report);
	}
	@Override
	public void onSkip(long newest,long bucket) {
		logger.warn(id + " skip: " + TopologyConstant.formatDatetime(bucket) + " newest: " + TopologyConstant.formatDatetime(newest));
		SpeakLoadSecond report = loadHistory.peekLast();
		if( report == null ) {
			report = new SpeakLoadSecond();
		}
		secondReport(bucket,report);
	}

	private void secondReport(long bucket,SpeakLoadSecond report) {
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

		SpeakLoadHour report = new SpeakLoadHour();

		report.speakingUsers = timeoutedUsers.values(start).size();
		report.speakingGroups = timeoutedGroups.values(start).size();
		report.lostAutoUsers = timeoutedLostAutoUsers.values(start).size();
		report.lostAutoGroups = timeoutedLostAutoGroups.values(start).size();
		report.lostReplaceUsers = timeoutedLostReplaceUsers.values(start).size();
		report.lostReplaceGroups = timeoutedLostReplaceGroups.values(start).size();
		report.dentUsers = timeoutedDentUsers.values(start).size();
		report.dentGroups = timeoutedDentGroups.values(start).size();

		for(SpeakLoadSecond sr : loadHistory) {
			report.speakingSeconds += sr.speakings;
			report.speakingTimes += sr.getTimes;
			report.lostAutoTimes += sr.lostAutoTimes;
			report.lostReplaceTimes += sr.lostReplaceTimes;
			report.dentTimes += sr.dentTimes;
		}

		receiver.onMinuteReport(id,minuteBucket,report);
	}
}

