package com.echat.storm.analysis.types;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.ITuple;

import com.echat.storm.analysis.constant.FieldConstant;
import com.echat.storm.analysis.constant.TopologyConstant;
import com.echat.storm.analysis.utils.BytesUtil;

public class TimeBucketReport {
	public String				entity;
	public String				type;
	public Long					bucket;
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
			bucket,
			content
		);
	}

	static public TimeBucketReport fromTuple(ITuple tuple) {
		TimeBucketReport report = new TimeBucketReport();
		report.entity = tuple.getString(0);
		report.type = tuple.getString(1);
		report.bucket = tuple.getLong(2);
		report.report = tuple.getString(3);
		return report;
	}

	static public byte[] rowKey(final String entity,long bucket) {
		byte[] entityPart = BytesUtil.stringToHashBytes(entity);
		byte[] bucketPart = BytesUtil.longToBytes(bucket);
		return BytesUtil.concatBytes(entityPart,bucketPart);
	}

	public Values toValues() {
		return new Values(entity,type,bucket,report);
	}
}

