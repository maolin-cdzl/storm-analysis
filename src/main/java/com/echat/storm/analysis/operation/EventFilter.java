package com.echat.storm.analysis.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import com.echat.storm.analysis.constant.FieldConstant;
import com.echat.storm.analysis.utils.DebugCounter;

public class EventFilter extends BaseFilter {
	private static final Logger logger = LoggerFactory.getLogger(EventFilter.class);
	private final String[] events;
	private DebugCounter _debug = new DebugCounter();

	public EventFilter(String event) {
		this.events = new String[]{ event };
	}

	public EventFilter(final String[] events) {
		this.events = events;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		_debug.countIn(logger,1);
		if( tuple.contains(FieldConstant.EVENT_FIELD) ) {
			final String event = tuple.getStringByField(FieldConstant.EVENT_FIELD);
			if( event != null ) {
				for(String ev : events) {
					if( ev.equals(event) ) {
						_debug.countOut(logger,1);
						return true;
					}
				}
			}
		}
		return false;
	}
}



