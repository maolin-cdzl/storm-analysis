package com.echat.storm.analysis.state;

import com.echat.storm.analysis.types.UserLoadSecond;
import com.echat.storm.analysis.types.UserLoadHour;

public interface IUserLoadReportReceiver {
	void onSecondReport(final String id,long bucket,UserLoadSecond report);
	void onMinuteReport(final String id,long bucket,UserLoadHour report);
}

