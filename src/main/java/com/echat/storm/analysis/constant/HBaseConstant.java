package com.echat.storm.analysis.constant;

public class HBaseConstant {
	// user action table
	public static final String USER_ACTION_TABLE = "user_action";
	public static final byte[] COLUMN_FAMILY_LOG = "l".getBytes();

	// user online session table
	public static final String USER_SESSION_TABLE = "user_session";

	// broken history table
	public static final String BROKEN_HISTORY_TABLE = "broken_history";

	// group event table
	public static final String GROUP_EVENT_TABLE = "group_event";
	public static final String TEMP_GROUP_EVENT_TABLE = "temp_group_event";
	
	// speaking table
	public static final String GROUP_SPEAKING_TABLE = "group_speaking";
	public static final String TEMP_GROUP_SPEAKING_TABLE = "temp_group_speaking";

	// server & company user load table
	public static final String SERVER_USER_LOAD_TABLE = "server_user_load";
	public static final String COMPANY_USER_LOAD_TABLE = "company_user_load";
	
	// server & company speak load table
	public static final String SERVER_SPEAK_LOAD_TABLE = "server_speak_load";
	public static final String COMPANY_SPEAK_LOAD_TABLE = "company_speak_load";
	

	// columns
	public static final byte[] COLUMN_SERVER = FieldConstant.SERVER_FIELD.getBytes();
	public static final byte[] COLUMN_DATETIME = FieldConstant.DATETIME_FIELD.getBytes();
	public static final byte[] COLUMN_EVENT = FieldConstant.EVENT_FIELD.getBytes();
	public static final byte[] COLUMN_UID = FieldConstant.UID_FIELD.getBytes();
	public static final byte[] COLUMN_GID = FieldConstant.GID_FIELD.getBytes();
	public static final byte[] COLUMN_COMPANY = FieldConstant.COMPANY_FIELD.getBytes();
	public static final byte[] COLUMN_AGENT = FieldConstant.AGENT_FIELD.getBytes();
	public static final byte[] COLUMN_CTX = FieldConstant.CTX_FIELD.getBytes();
	public static final byte[] COLUMN_IP = FieldConstant.IP_FIELD.getBytes();
	public static final byte[] COLUMN_DEVICE = FieldConstant.DEVICE_FIELD.getBytes();
	public static final byte[] COLUMN_DEVICE_ID = FieldConstant.DEVICE_ID_FIELD.getBytes();
	public static final byte[] COLUMN_VERSION = FieldConstant.VERSION_FIELD.getBytes();
	public static final byte[] COLUMN_IMSI = FieldConstant.IMSI_FIELD.getBytes();
	public static final byte[] COLUMN_EXPECT_PAYLOAD = FieldConstant.EXPECT_PAYLOAD_FIELD.getBytes();
	public static final byte[] COLUMN_TARGET = FieldConstant.TARGET_FIELD.getBytes();
	public static final byte[] COLUMN_TARGET_GOT = FieldConstant.TARGET_GOT_FIELD.getBytes();
	public static final byte[] COLUMN_TARGET_DENT = FieldConstant.TARGET_DENT_FIELD.getBytes();
	public static final byte[] COLUMN_COUNT = FieldConstant.COUNT_FIELD.getBytes();
	public static final byte[] COLUMN_SW = FieldConstant.SW_FIELD.getBytes();
	public static final byte[] COLUMN_VALUE = FieldConstant.VALUE_FIELD.getBytes();

	public static final byte[] COLUMN_TIME_START = FieldConstant.TIME_START_FIELD.getBytes();
	public static final byte[] COLUMN_TIME_END = FieldConstant.TIME_END_FIELD.getBytes();


	public static final byte[] COLUMN_OFFLINE_TIME = FieldConstant.OFFLINE_TIME_FIELD.getBytes();

	public static final byte[] COLUMN_GROUP_TYPE = FieldConstant.GROUP_TYPE_FIELD.getBytes();
	public static final byte[] COLUMN_GROUP_COMPANY = FieldConstant.GROUP_COMPANY_FIELD.getBytes();
	public static final byte[] COLUMN_GROUP_AGENT = FieldConstant.GROUP_AGENT_FIELD.getBytes();


	public static final byte[] COLUMN_REPORT = FieldConstant.REPORT_FIELD.getBytes();
}

