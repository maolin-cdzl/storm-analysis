package com.echat.storm.analysis.state;

import com.echat.storm.analysis.types.SpeakLoadSecond;
import com.echat.storm.analysis.types.SpeakLoadHour;

public interface ISpeakLoadReportReceiver {
	void onSecondReport(final String id,long bucket,SpeakLoadSecond report);
	void onMinuteReport(final String id,long bucket,SpeakLoadHour report);
}


