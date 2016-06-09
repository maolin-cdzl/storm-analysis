package com.echat.storm.analysis.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFilter;

import com.echat.storm.analysis.constant.FieldConstant;

public class FieldFilter extends BaseFilter {
	private final String field;

	public FieldFilter(final String field) {
		this.field = field;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		if( tuple.contains(field) ) {
			return (null != tuple.getValueByField(field));
		} else {
			return false;
		}
	}
}


