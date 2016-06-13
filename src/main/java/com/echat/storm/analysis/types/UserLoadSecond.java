package com.echat.storm.analysis.types;

public class UserLoadSecond {
	public int					onlines = 0;
	public int					loginTimes = 0;
	public int					logoutTimes = 0;
	public int					brokenTimes = 0;

	public UserLoadSecond merge(final UserLoadSecond other) {
		onlines += other.onlines;
		loginTimes += other.loginTimes;
		logoutTimes += other.logoutTimes;
		brokenTimes += other.brokenTimes;
		return this;
	}
}
