package com.echat.storm.analysis.types;

public class GroupEvent {
	public String server;
	public String datetime;
	public String event;
	public String uid;
	public String company;
	public String agent;
	public String gid;
	public String group_type;
	public String group_company;
	public String group_agent;

	// no transaction member
	private transient long timestamp = 0;

	static public Fields getFields() {
		return new Fields(
				FieldConstant.SERVER_FIELD,
				FieldConstant.DATETIME_FIELD,
				FieldConstant.EVENT_FIELD,
				FieldConstant.UID_FIELD,
				FieldConstant.COMPANY_FIELD,
				FieldConstant.AGENT_FIELD,
				FieldConstant.GID_FIELD,
				FieldConstant.GROUP_TYPE_FIELD,
				FieldConstant.GROUP_COMPANY_FIELD,
				FieldConstant.GROUP_AGENT_FIELD
				);
	}

	static public GroupEvent fromTuple(ITuple tuple) {
		GroupEvent ev = new GroupEvent();
		ev.server = tuple.getString(0);
		ev.datetime = tuple.getString(1);
		ev.event = tuple.getString(2);
		ev.uid = tuple.getString(3);
		ev.company = tuple.getString(4);
		ev.agent = tuple.getString(5);
		ev.gid = tuple.getString(6);
		ev.group_type = tuple.getString(7);
		ev.group_company = tuple.getString(8);
		ev.group_agent = tuple.getString(9);

		return ev;
	}

	public Values toValues() {
		return new Values(
			server,
			datetime,
			event,
			uid,
			company,
			agent,
			gid,
			group_type,
			group_company,
			group_agent
		);
	}

	public Date getDate() {
		return TopologyConstant.parseDatetime(datetime);
	}
	public long getTimeStamp() {
		if( timestamp == 0 ) {
			timestamp = getDate().getTime();
		}
		return timestamp;
	}
}

