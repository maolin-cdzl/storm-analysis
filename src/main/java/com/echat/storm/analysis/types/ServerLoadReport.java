package com.echat.storm.analysis.types;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.echat.storm.analysis.constant.FieldConstant;
import com.echat.storm.analysis.constant.TopologyConstant;

public class ServerLoadReport {
	public String				server;
	public String				type;
	public String				bucket;

	public String				report;

	static public Fields getFields() {
		return new Fields(
			FieldConstant.SERVER_FIELD,
			FieldConstant.REPORT_TYPE,
			FieldConstant.REPORT_BUCKET,
			FieldConstant.REPORT_CONTENT
		);
	}

	static public Values makeReport(final String server,final String type,long bucket,final String content) {
		return new Values(
			server,type,
			TopologyConstant.formatDatetime(bucket),
			content
		);
	}

	public Values toValues() {
		return new Values(server,type,bucket,report);
	}
}

