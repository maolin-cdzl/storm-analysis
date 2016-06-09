package com.echat.storm.analysis.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFilter;

import java.util.Arrays;

import com.echat.storm.analysis.constant.FieldConstant;

public class EventFilter extends BaseFilter {
	private final String[] events;

	public EventFilter(String event) {
		this.events = new String[]{ event };
	}

	public EventFilter(final String[] events) {
		this.events = events;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		if( tuple.contains(FieldConstant.EVENT_FIELD) ) {
			final String event = tuple.getStringByField(FieldConstant.EVENT_FIELD);
			if( event != null ) {
				for(String ev : events) {
					if( ev.equals(event) ) {
						return true;
					}
				}
			}
		}
		return false;
	}
}



