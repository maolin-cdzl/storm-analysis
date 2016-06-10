package com.echat.storm.analysis.types;

import org.apache.hadoop.hbase.client.Put;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.utils.BytesUtil;

public class UserSession {
	private static final Logger logger = LoggerFactory.getLogger(UserSession.class);

	public String			server;
	public String			uid;
	public String			company;
	public String			agent;
	public String			login;
	public String			logout;
	public String			ctx;
	public String			ip;
	public String			device;
	public String			devid;
	public String			version;
	public String			imsi;
	public String			expect_payload;

	private transient long loginTs = 0;
	private transient long logoutTs = 0;

	static public UserSession create(UserOnlineEvent login,UserOnlineEvent logout) {
		if( !login.uid.equals(logout.uid) ) {
			logger.error("Unpaired uid: " + login.uid + " " + logout.uid);
			return null;
		}
		if( ! login.server.equals(logout.server) ) {
			logger.warn("uid=" + login.uid + " login at " + login.server + ", logout at " + logout.server);
			return null;
		}
		if( login.getTimeStamp() > logout.getTimeStamp() ) {
			logger.warn("uid=" + login.uid + " login " + login.datetime + " after logout " + logout.datetime);
		}

		UserSession session = new UserSession();
		session.server = login.server;
		session.uid = login.uid;
		session.company = login.company;
		session.agent = login.agent;
		session.login = login.datetime;
		session.logout = logout.datetime;
		session.ctx = login.ctx;
		session.ip = login.ip;
		session.device = login.device;
		session.devid = login.devid;
		session.version = login.version;
		session.imsi = login.imsi;
		session.expect_payload = login.expect_payload;

		session.loginTs = login.getTimeStamp();
		session.logoutTs = logout.getTimeStamp();

		return session;
	}

	public Date getLoginDate() {
		return TopologyConstant.parseDatetime(login);
	}
	public long getLoginTimeStamp() {
		if( loginTs == 0 ) {
			loginTs = getLoginDate().getTime();
		}
		return loginTs;
	}
	public Date getLogoutDate() {
		return TopologyConstant.parseDatetime(logout);
	}
	public long getLogoutTimeStamp() {
		if( logoutTs == 0 ) {
			logoutTs = getLogoutDate().getTime();
		}
		return logoutTs;
	}

	public Put toRow() {
		Put row = new Put(rowKey());

		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SERVER,server.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_UID,uid.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COMPANY,company.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TIME_START,login.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TIME_END,logout.getBytes());

		if( agent != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_AGENT,agent.getBytes());
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
		return row;
	}

	private byte[] rowKey() {
		byte[] companyPart = BytesUtil.stringToHashBytes(company);
		byte[] loginPart = BytesUtil.longToBytes( getLoginTimeStamp() );
		byte[] logoutPart = BytesUtil.longToBytes( getLogoutTimeStamp() );
		byte[] userPart = BytesUtil.stringToHashBytes(uid);

		return BytesUtil.concatBytes(companyPart,loginPart,logoutPart,userPart);
	}
}


