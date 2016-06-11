package com.echat.storm.analysis.utils;

public interface ITimeBucketSlidingWindowCallback<T> {
	T createItem(long bucket);
	void onSlidingOut(long bucket,T val);
	void onSkip(long bucket);
}
