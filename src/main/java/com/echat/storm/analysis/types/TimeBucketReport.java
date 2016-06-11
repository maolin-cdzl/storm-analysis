package com.echat.storm.analysis.types;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.echat.storm.analysis.constant.FieldConstant;
import com.echat.storm.analysis.constant.TopologyConstant;

public class TimeBucketReport {
	public String				entity;
	public String				type;
	public String				bucket;

	public String				report;

	static public Fields getFields() {
		return new Fields(
			FieldConstant.REPORT_ENTITY_FIELD,
			FieldConstant.REPORT_TYPE_FIELD,
			FieldConstant.REPORT_BUCKET_FIELD,
			FieldConstant.REPORT_CONTENT_FIELD
		);
	}

	static public Values makeReport(final String entity,final String type,long bucket,final String content) {
		return new Values(
			entity,type,
			TopologyConstant.formatDatetime(bucket),
			content
		);
	}

	public Values toValues() {
		return new Values(entity,type,bucket,report);
	}
}

