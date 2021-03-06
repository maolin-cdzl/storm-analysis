package com.echat.storm.analysis.types;

public class UserLoadHour {
	public int					onlines = 0;
	public int					loginUsers = 0;
	public int					loginTimes = 0;
	public int					logoutUsers = 0;
	public int					logoutTimes = 0;
	public int					brokenUsers = 0;
	public int					brokenTimes = 0;
	public long					totalOnlineSeconds = 0L;		// 一小时所有用户的在线总时长(秒)

	public UserLoadHour merge(final UserLoadHour other) {
		onlines += other.onlines;
		loginUsers += other.loginUsers;
		loginTimes += other.loginTimes;
		logoutUsers += logoutUsers;
		logoutTimes += logoutTimes;
		brokenUsers += brokenUsers;
		brokenTimes += brokenTimes;
		totalOnlineSeconds += totalOnlineSeconds;
		return this;
	}
}

