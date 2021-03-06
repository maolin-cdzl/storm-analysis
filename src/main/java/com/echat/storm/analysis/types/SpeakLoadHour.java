package com.echat.storm.analysis.types;

import org.apache.hadoop.hbase.client.Put;

public class SpeakLoadHour {
	public long					speakingSeconds;	// 发言总时长
	public int					speakingTimes;		// 一小时一共有多少发言
	public int					speakingUsers;		// 有多少用户发言过
	public int					speakingGroups;		// 有多少群组有过发言
	public int					lostAutoTimes;		// 一共多少次服务器摘麦
	public int					lostAutoUsers;		// 有多少用户被服务器摘麦
	public int					lostAutoGroups;		// 有多少群组发生过摘麦
	public int					lostReplaceTimes;	// 一共多少次被抢麦
	public int					lostReplaceUsers;	// 有多少用户被抢麦
	public int					lostReplaceGroups;	// 有多少群组发生过被抢麦
	public int					dentTimes;			// 一共多少次抢麦失败
	public int					dentUsers;			// 有多少用户抢麦失败
	public int					dentGroups;			// 有多少群组发生过抢麦失败


	public SpeakLoadHour merge(final SpeakLoadHour other) {
		speakingSeconds += other.speakingSeconds;
		speakingTimes += other.speakingTimes;
		speakingUsers += other.speakingUsers;
		speakingGroups += other.speakingGroups;
		lostAutoTimes += other.lostAutoTimes;
		lostAutoUsers += other.lostAutoUsers;
		lostAutoGroups += other.lostAutoGroups;
		lostReplaceTimes += other.lostReplaceTimes;
		lostReplaceUsers += other.lostReplaceUsers;
		lostReplaceGroups += other.lostReplaceGroups;
		dentTimes += other.dentTimes;
		dentUsers += other.dentUsers;
		dentGroups += other.dentGroups;
		return this;
	}
}

