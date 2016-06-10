package com.echat.storm.analysis.types;

import java.util.Date;
import java.text.ParseException;
import org.apache.commons.lang.time.DateUtils;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.ITuple;
import com.echat.storm.analysis.constant.FieldConstant;
import com.echat.storm.analysis.constant.TopologyConstant;

import org.apache.hadoop.hbase.client.Put;
import com.echat.storm.analysis.utils.BytesUtil;

import com.echat.storm.analysis.constant.*;

public class UserActionEvent {
	public String server;
	public String datetime;
	public String event;
	public String uid;
	public String gid;
	public String company;
	public String agent;
	public String ctx;
	public String ip;
	public String device;
	public String devid;
	public String version;
	public String imsi;
	public String expect_payload;
	public String target;
	public String target_got;
	public String target_dent;
	public String count;
	public String sw;
	public String value;

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
				FieldConstant.CTX_FIELD,
				FieldConstant.IP_FIELD,
				FieldConstant.DEVICE_FIELD,
				FieldConstant.DEVICE_ID_FIELD,
				FieldConstant.VERSION_FIELD,
				FieldConstant.IMSI_FIELD,
				FieldConstant.EXPECT_PAYLOAD_FIELD,
				FieldConstant.TARGET_FIELD,
				FieldConstant.TARGET_GOT_FIELD,
				FieldConstant.TARGET_DENT_FIELD,
				FieldConstant.COUNT_FIELD,
				FieldConstant.SW_FIELD,
				FieldConstant.VALUE_FIELD
					);
	}
	
	static public UserActionEvent fromTuple(ITuple tuple) {
		UserActionEvent log = new UserActionEvent();
		log.server = tuple.getString(0);
		log.datetime = tuple.getString(1);
		log.event = tuple.getString(2);
		log.uid = tuple.getString(3);
		log.gid = tuple.getString(4);
		log.company = tuple.getString(5);
		log.agent = tuple.getString(6);
		log.ctx = tuple.getString(7);
		log.ip = tuple.getString(8);
		log.device = tuple.getString(9);
		log.devid = tuple.getString(10);
		log.version = tuple.getString(11);
		log.imsi = tuple.getString(12);
		log.expect_payload = tuple.getString(13);
		log.target = tuple.getString(14);
		log.target_got = tuple.getString(15);
		log.target_dent = tuple.getString(16);
		log.count = tuple.getString(17);
		log.sw = tuple.getString(18);
		log.value = tuple.getString(19);
		return log;
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
			ctx,
			ip,
			device,
			devid,
			version,
			imsi,
			expect_payload,
			target,
			target_got,
			target_dent,
			count,
			sw,
			value
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

	public Put toRow() {
		Put row = new Put(rowKey());

		// must exists column
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SERVER,server.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DATETIME,datetime.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_EVENT,event.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_UID,uid.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COMPANY,company.getBytes());

		if( agent != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_AGENT,agent.getBytes());
		}
		if( gid != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GID,gid.getBytes());
		}
		if( ctx != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_CTX,ctx.getBytes());
		}
		if( ip != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_IP,ip.getBytes());
		}
		if( device != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DEVICE,device.getBytes());
		}
		if( devid != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DEVICE_ID,devid.getBytes());
		}
		if( version != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_VERSION,version.getBytes());
		}
		if( imsi != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_IMSI,imsi.getBytes());
		}
		if( expect_payload != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_EXPECT_PAYLOAD,expect_payload.getBytes());
		}
		if( target != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TARGET,target.getBytes());
		}
		if( target_got != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TARGET_GOT,target_got.getBytes());
		}
		if( target_dent != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TARGET_DENT,target_dent.getBytes());
		}
		if( count != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COUNT,count.getBytes());
		}
		if( sw != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SW,sw.getBytes());
		}
		if( value != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_VALUE,value.getBytes());
		}
		return row;
	}

	public byte[] rowKey() {
		byte[] companyPart = BytesUtil.stringToHashBytes(company);
		byte[] userPart = BytesUtil.stringToHashBytes(uid);
		byte[] tsPart = BytesUtil.longToBytes(getTimeStamp());
		byte[] evPart = BytesUtil.stringToHashBytes(event);

		return BytesUtil.concatBytes(companyPart,userPart,tsPart,evPart);
	}
}

