package com.echat.storm.analysis.types;

public class SpeakLoadSecond {
	public int						speakings;			// 正在发言的数量
	public int						getTimes;		// 新发言
	public int						lostAutoTimes;		// 被服务器摘麦的次数
	public int						lostReplaceTimes;	// 被抢麦的次数
	public int						dentTimes;			// 被拒绝的话权申请次数
}

