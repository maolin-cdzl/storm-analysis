package com.echat.storm.analysis.constant;

public class EventConstant {
	// speak
	public static final String EVENT_GET_MIC = "GET_MIC";
	public static final String EVENT_RELEASE_MIC = "RELEASE_MIC";
	public static final String EVENT_DENT_MIC = "DENT_MIC";
	public static final String EVENT_LOSTMIC_AUTO = "LOSTMIC_AUTO";
	public static final String EVENT_LOSTMIC_REPLACE = "LOSTMIC_REPLACE";

	// online
	public static final String EVENT_RELOGIN = "RELOGIN";
	public static final String EVENT_LOGIN = "LOGIN";
	public static final String EVENT_LOGOUT = "LOGOUT";
	public static final String EVENT_BROKEN = "BROKEN";

	// login failed
	public static final String EVENT_LOGIN_FAILED = "LOGIN_FAILED";

	// group in out
	public static final String EVENT_JOIN_GROUP = "JOIN_GROUP";
	public static final String EVENT_LEAVE_GROUP = "LEAVE_GROUP";

	// call
	public static final String EVENT_CALL = "CALL";
	public static final String EVENT_QUICKDIAL = "QUICK_DIAL";

	// query
	public static final String EVENT_QUERY_MEMBERS = "QUERY_MEMBERS";
	public static final String EVENT_QUERY_GROUP = "QUERY_GROUP";
	public static final String EVENT_QUERY_CONTACT = "QUERY_CONTACT";
	public static final String EVENT_QUERY_USER = "QUERY_USER";
	public static final String EVENT_QUERY_DEPARTMENT = "QUERY_DEPARTMENT";
	public static final String EVENT_QUERY_ENTERPISE_GROUP = "QUERY_ENTERPRISE_GROUP";

	// profile
	public static final String EVENT_CHANGE_NAME = "CH_NAME";
	public static final String EVENT_CHANGE_PWD = "CH_PWD";
	public static final String EVENT_CHANGE_PWD_FAILED = "CH_PWD_FAILED";
	public static final String EVENT_CONTACT_REQ = "CONTACT_REQ";
	public static final String EVENT_CONTACT_REP = "CONTACT_REP";
	public static final String EVENT_CONTACT_RM = "CONTACT_RM";
	
	// manage
	public static final String EVENT_DISPATCH = "DISPATCH";
	public static final String EVENT_CONFIG = "CONFIG"; // current contains SW_AUDIO and SW_GPS
	public static final String EVENT_SW_AUDIO = "SW_AUDIO";
	public static final String EVENT_SW_GPS = "SW_GPS";
	public static final String EVENT_TAKE_MIC = "TAKE_MIC";
	public static final String EVENT_CREATE_GROUP = "CREATE_GROUP";
	public static final String EVENT_RM_GROUP = "RM_GROUP";
	public static final String EVENT_EMPOWER = "EMPOWER";
	public static final String EVENT_EMPOWER_FAILED = "EMPOWER_FAILED";
	public static final String EVENT_DEPRIVE = "DEPRIVE";
	public static final String EVENT_DEPRIVE_FAILED = "DEPRIVE_FAILED";
	public static final String EVENT_CHANGE_GROUP_NAME = "CH_GROUP_NAME";
	public static final String EVENT_CHANGE_GROUP_NAME_FAILED = "CH_GROUP_NAME_FAILED";
	
	// worksheet
	public static final String EVENT_WORKSHEET_POST = "WORKSHEET_POST";

}


