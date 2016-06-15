package com.echat.storm.analysis.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import com.echat.storm.analysis.constant.FieldConstant;

public class FieldFilter extends BaseFilter {
	private static final Logger logger = LoggerFactory.getLogger(FieldFilter.class);
	private final String field;

	public FieldFilter(final String field) {
		this.field = field;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		logger.info("[prepare] partitionIndex:{}",context.getPartitionIndex());
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


