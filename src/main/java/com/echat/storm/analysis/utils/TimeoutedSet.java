package com.echat.storm.analysis.utils;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;

public class TimeoutedSet<T extends Comparable> {
	private class TimeoutedItem {
		public long			timeout;
		public T			value;

		public TimeoutedItem(long t,T v) {
			timeout = t;
			value = v;
		}
	}

	private class TimeoutedItemComparator implements Comparator<TimeoutedItem> {
		@Override
		public int compare(TimeoutedItem item1,TimeoutedItem item2) {
			final long diff = item1.timeout - item2.timeout;
			if( diff > 0 ) {
				return 1;
			} else if( diff < 0 ) {
				return -1;
			} else {
				return item1.value.compareTo(item2.value);
			}
		}
	}

	private HashMap<T,TimeoutedItem>				_items;
	private SortedLinkedList<TimeoutedItem>			_index;

	public TimeoutedSet() {
		_items = new HashMap<T,TimeoutedItem>();
		_index = new SortedLinkedList<TimeoutedItem>(new TimeoutedItemComparator());
	}

	// put a item without timeout,if it exists,will be replaced even exists one has timeout setting.
	public T put(T v) {
		TimeoutedItem item = _items.get(v);
		if( item != null ) {
			if( item.timeout != Long.MAX_VALUE ) {
				_index.remove(item);
				item.timeout = Long.MAX_VALUE;
				_index.add(item);
			}
		} else {
			item = new TimeoutedItem(Long.MAX_VALUE,v);
			_items.put(v,item);
			_index.add(item);
		}
		return v;
	}

	public T put(T v,long timeout) {
		TimeoutedItem item = _items.get(v);
		if( item != null ) {
			if( item.timeout != timeout ) {
				_index.remove(item);
				item.timeout = timeout;
				_index.add(item);
			}
		} else {
			item = new TimeoutedItem(timeout,v);
			_items.put(v,item);
			_index.add(item);
		}
		return v;
	}

	public void cleanup(final long now) {
		TimeoutedItem item;
		while( ! _index.isEmpty() ) {
			item = _index.peekFirst();
			if( item.timeout <= now ) {
				_index.pollFirst();
				_items.remove(item.value);
			} else {
				break;
			}
		}
	}

	public Set<T> values() {
		return _items.keySet();
	}

	public Set<T> values(final long now) {
		cleanup(now);
		return _items.keySet();
	}



}
