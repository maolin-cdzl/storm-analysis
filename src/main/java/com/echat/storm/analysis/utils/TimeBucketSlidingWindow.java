package com.echat.storm.analysis.utils;

import java.util.List;
import java.util.LinkedList;

public class TimeBucketSlidingWindow<T> {
	private class TimeBucketItem {
		public final long		bucket;
		public T				value;

		public TimeBucketItem(final long b) {
			bucket = b;
			value = null;
		}

		public TimeBucketItem(final long b,T v) {
			bucket = b;
			value = v;
		}
	}

	private final long								_bucketWidth;
	private final long								_shelfLife;
	private ITimeBucketSlidingWindowCallback<T>		_callback;
	private LinkedList<TimeBucketItem>				_timeline;

	public TimeBucketSlidingWindow(long bucketWidth,long shelfLife,ITimeBucketSlidingWindowCallback<T> cb) {
		if( shelfLife % bucketWidth != 0 ) {
			throw new IllegalArgumentException("shelfLife must be a multiple number of bucketWidth");
		}
		_bucketWidth = bucketWidth;
		_shelfLife = shelfLife;
		_callback = cb;
		_timeline = new LinkedList<TimeBucketItem>();
	}

	public T get(long bucket) {
		bucket = bucket / _bucketWidth * _bucketWidth;

		if( _timeline.isEmpty() ) {
			// init
			for(long b = bucket - _shelfLife - _bucketWidth; b <= bucket; b += _bucketWidth) {
				_timeline.add( new TimeBucketItem(b,_callback.createItem(b)) );
			}
			return _timeline.peekLast().value;
		}

		for(TimeBucketItem item : _timeline) {
			if( item.bucket == bucket ) {
				return item.value;
			} else if( item.bucket > bucket ) {
				// this bucket already timeouted
				return null;
			}
		}

		// fresh than any exists
		final long start = bucket - _shelfLife - _bucketWidth;
		long step = _timeline.peekFirst().bucket;

		while( !_timeline.isEmpty() && step < start ) {
			step = _timeline.peekFirst().bucket;
			if( step < start ) {
				TimeBucketItem item = _timeline.pollFirst();
				_callback.onSlidingOut(item.bucket,item.value);
			} else {
				break;
			}
		}

		// jump too much
		while( step < start ) {
			step += _bucketWidth;
			_callback.onSkip(bucket,step);
		}

		if( ! _timeline.isEmpty() ) {
			step = _timeline.peekLast().bucket;
		}
		
		while( step < bucket ) {
			step += _bucketWidth;
			_timeline.add( new TimeBucketItem(step,_callback.createItem(step)) );
		}
		return _timeline.peekLast().value;
	}
}

