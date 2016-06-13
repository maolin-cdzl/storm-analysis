package com.echat.storm.analysis.types;

import org.apache.hadoop.hbase.client.Put;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.utils.BytesUtil;

public class SpeakingRecord {
	public static class Builder {
		private final String	_server;
		private final String	_gid;
		private final String	_group_type;
		private final String	_group_company;
		private final String	_group_agent;

		private String			_uid;
		private String			_user_company;
		private String			_user_agent;
		private String			_start_speak;

		public Builder(final GroupEvent ev) {
			_server = ev.server;
			_gid = ev.getGroupFullId();
			_group_type = ev.group_type;
			_group_company = ev.group_company;
			_group_agent = ev.group_agent;
			reset();
		}

		public SpeakingRecord startSpeak(final GroupEvent ev) {
			SpeakingRecord record = null;
			if( _uid != null ) {
				record = makeRecord(ev);
			}
			_uid = ev.uid;
			_user_company = ev.company;
			_user_agent = ev.agent;
			_start_speak = ev.datetime;
			return record;
		}

		public SpeakingRecord stopSpeak(final GroupEvent ev) {
			if( _uid == null || ! _uid.equals(ev.uid) ) {
				reset();
				return null;
			}
			SpeakingRecord record = makeRecord(ev);
			return record;
		}

		private SpeakingRecord makeRecord(final GroupEvent ev) {
			SpeakingRecord record = new SpeakingRecord();
			record.server = _server;
			record.gid = _gid;
			record.group_type = _group_type;
			record.group_company = _group_company;
			record.group_agent = _group_agent;
			record.uid = _uid;
			record.user_company = _user_company;
			record.user_agent = _user_agent;
			record.start_time = _start_speak;
			record.end_time = ev.datetime;
			record.end_event = ev.event;
			reset();
			return record;
		}

		private void reset() {
			_uid = null;
			_user_company = null;
			_user_agent = null;
			_start_speak = null;
		}
	}

	static public Builder createBuilder(final GroupEvent ev) {
		return new Builder(ev);
	}

	public String				server;
	public String				gid;
	public String				group_type;
	public String				group_company;
	public String				group_agent;
	public String				uid;
	public String				user_company;
	public String				user_agent;
	public String				start_time;
	public String				end_time;
	public String				end_event;


	private transient long startTs = 0;
	private transient long endTs = 0;

	public Date getStartDate() {
		return TopologyConstant.parseDatetime(start_time);
	}
	public long getStartTimeStamp() {
		if( startTs == 0 ) {
			startTs = getStartDate().getTime();
		}
		return startTs;
	}
	public Date getEndDate() {
		return TopologyConstant.parseDatetime(end_time);
	}
	public long getEndTimeStamp() {
		if( endTs == 0 ) {
			endTs = getEndDate().getTime();
		}
		return endTs;
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


	public byte[] rowKey() {
		byte[] companyPart = BytesUtil.stringToHashBytes(group_company);
		byte[] gidPart = BytesUtil.stringToHashBytes(gid);
		byte[] startPart = BytesUtil.longToBytes(getStartTimeStamp());
		byte[] endPart = BytesUtil.longToBytes(getEndTimeStamp());
		byte[] uidPart = BytesUtil.stringToHashBytes(uid);

		return BytesUtil.concatBytes(companyPart,gidPart,startPart,endPart,uidPart);
	}

	public byte[] tempRowKey() {
		byte[] companyPart = BytesUtil.stringToHashBytes(group_company);
		byte[] startPart = BytesUtil.longToBytes(getStartTimeStamp());
		byte[] endPart = BytesUtil.longToBytes(getEndTimeStamp());
		byte[] uidPart = BytesUtil.stringToHashBytes(uid);
		byte[] gidPart = BytesUtil.stringToHashBytes(gid);

		return BytesUtil.concatBytes(companyPart,startPart,endPart,uidPart,gidPart);
	}

	public void toRow(Put row) {
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SERVER,server.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GID,gid.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GROUP_TYPE,group_type.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GROUP_COMPANY,group_company.getBytes());
		if( group_agent != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GROUP_AGENT,group_agent.getBytes());
		}
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_UID,uid.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COMPANY,user_company.getBytes());
		if( user_agent != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_AGENT,user_agent.getBytes());
		}
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TIME_START,start_time.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TIME_END,end_time.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_EVENT,end_event.getBytes());
	}
}

