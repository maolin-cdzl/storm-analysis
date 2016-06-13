package com.echat.storm.analysis.types;

import java.util.Date;
import java.text.ParseException;
import org.apache.commons.lang.time.DateUtils;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.ITuple;

import org.apache.hadoop.hbase.client.Put;

import com.echat.storm.analysis.constant.*;

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

	public String getGroupFullId() {
		if( ValueConstant.GROUP_TYPE_TEMP.equals(type) ) {
			return server + ":" + gid;
		} else {
			return gid;
		}
	}

	public Put toRow() {
		Put row = new Put(rowKey());
		toRow(row);
		return row;
	}

	public Put toTempRow() {
		Put row = new Put(tempRowKey());
		toRow(row);
		return row;
	}

	public void toRow(Put row) {
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SERVER,server.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DATETIME,datetime.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_EVENT,event.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_UID,uid.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COMPANY,company.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GID,gid.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GROUP_TYPE,type.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GROUP_COMPANY,group_company.getBytes());

		if( agent != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_AGENT,agent.getBytes());
		}
		if( group_agent != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GROUP_AGENT,group_agent.getBytes());
		}
	}
	public byte[] rowKey() {
		byte[] companyPart = BytesUtil.stringToHashBytes(group_company);
		byte[] gidPart = BytesUtil.stringToHashBytes(gid);
		byte[] tsPart = BytesUtil.longToBytes(getTimeStamp());
		byte[] evPart = BytesUtil.stringToHashBytes(event);

		return BytesUtil.concatBytes(companyPart,gidPart,tsPart,evPart);
	}

	public byte[] tempRowKey() {
		byte[] companyPart = BytesUtil.stringToHashBytes(group_company);
		byte[] tsPart = BytesUtil.longToBytes(getTimeStamp());
		byte[] serverPart = BytesUtil.stringToHashBytes(server);
		byte[] gidPart = BytesUtil.stringToHashBytes(gid);
		byte[] evPart = BytesUtil.stringToHashBytes(event);

		return BytesUtil.concatBytes(companyPart,tsPart,serverPart,gidPart,evPart);
	}

}

