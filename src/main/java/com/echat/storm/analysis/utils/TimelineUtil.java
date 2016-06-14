package com.echat.storm.analysis.utils;

import java.util.Collection;
import java.util.Set;
import java.util.Map;

public class TimelineUtil<T> {
	public class Value {
		public long			time;
		public T			val;
	}

	private LRUHashMap<String,Value> timelineMap;

	public TimelineUtil(int cacheSize) {
		timelineMap = new LRUHashMap<String,Value>(cacheSize);
	}

	public Value get(final String key) {
		return timelineMap.get(key);
	}

	public boolean update(final String key,long tl,T val) {
		Value last = timelineMap.get(key);
		if( last == null ) {
			last = new Value();
			last.time = tl;
			last.val = val;
			timelineMap.put(key,last);
			return true;
		} else if(last.time <= tl ) {
			last.time = tl;
			last.val = val;
			return true;
		} else {
			return false;
		}
	}

	public boolean isExpired(final String key,long tl) {
		final Value last = timelineMap.get(key);
		if( last != null && last.time > tl ) {
			return true;
		} else {
			return false;
		}
	}

	public boolean before(final String key1,final String key2) {
		Value t1 = get(key1);
		Value t2 = get(key2);
		if( t2 == null ) {
			return true;
		} else if( t1 == null ) {
			return false;
		} else {
			return t1.time < t2.time;
		}
	}

	public Set<String> keySet() {
		return timelineMap.keySet();
	}

	public Collection<Value> values() {
		return timelineMap.values();
	}

	public Set<Map.Entry<String,Value>> entrySet() {
		return timelineMap.entrySet();
	}
}

