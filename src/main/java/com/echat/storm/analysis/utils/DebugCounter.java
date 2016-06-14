package com.echat.storm.analysis.utils;

import java.io.Serializable;
import org.slf4j.Logger;

import com.echat.storm.analysis.constant.TopologyConstant;

public class DebugCounter implements Serializable {
	private long _lastReport = 0;
	private long _countIn = 0;
	private long _countOut = 0;
	private long _totalIn = 0;
	private long _totalOut = 0;


	public void countIn(final Logger logger,int c) {
		if( TopologyConstant.DEBUG ) {
			_countIn += c;
			checkReport(logger);
		}
	}

	public void countOut(final Logger logger,int c) {
		if( TopologyConstant.DEBUG ) {
			_countOut += c;
			checkReport(logger);
		}
	}

	private void checkReport(final Logger logger) {
		final long now = System.currentTimeMillis();
		if( _lastReport == 0 ) {
			_lastReport = now;
		} else if( now - _lastReport >= TopologyConstant.LOG_REPORT_PERIOD ) {
			_totalIn += _countIn;
			_totalOut += _countOut;
			logger.info("Period:" + (now - _lastReport)
					+ " in:" + _countIn + " out:" + _countOut
					+ " totalIn:" + _totalIn + " totalOut:" + _totalOut);
			_countIn = 0;
			_countOut = 0;
			_lastReport = now;
		}
	}
}

