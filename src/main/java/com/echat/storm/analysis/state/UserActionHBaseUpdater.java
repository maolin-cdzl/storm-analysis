package com.echat.storm.analysis.state;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.BaseStateUpdater;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class UserActionHBaseUpdater extends BaseStateUpdater<BaseState> {
	private static final Logger logger = LoggerFactory.getLogger(UserActionHBaseUpdater.class);

	private HTableInterface _table = null;


	@Override
	public void updateState(BaseState state, List<TridentTuple> inputs,TridentCollector collector) {
		List<Put> puts = new ArrayList<Put>();

		for(TridentTuple tuple : inputs) {
			UserActionEvent ev = UserActionEvent.fromTuple(tuple);
			puts.add(createRow(ev));
		}
		HTableInterface table = getHTable(state);
		if( table == null ) {
			logger.error("Can not get htable instance");
			return;
		}
		Object[] result = new Object[puts.size()];
        try {
            table.batch(puts, result);
        } catch (InterruptedException e) {
            logger.error("Error performing a put to HBase.", e);
			returnHTable(state);
        } catch (IOException e) {
            logger.error("Error performing a put to HBase.", e);
			returnHTable(state);
        }
	}

	private HTableInterface getHTable(BaseState state) {
		if( _table == null ) {
			_table = state.getHTable(HBaseConstant.USER_ACTION_TABLE);
		}
		return _table;
	}
	private void returnHTable(BaseState state) {
		if( _table != null ) {
			state.returnHTable(_table);
			_table = null;
		}
	}

	private byte[] rowKey(UserActionEvent ev) {
		byte[] companyPart = BytesUtil.stringToHashBytes(ev.company);
		byte[] userPart = BytesUtil.stringToHashBytes(ev.uid);
		byte[] tsPart = BytesUtil.longToBytes(ev.getDate().getTime());
		byte[] evPart = BytesUtil.stringToHashBytes(ev.event);

		return BytesUtil.concatBytes(companyPart,userPart,tsPart,evPart);
	}

    private Put createRow(UserActionEvent ev) {
		Put row = new Put(rowKey(ev));

		// must exists column
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SERVER,ev.server.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DATETIME,ev.datetime.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_EVENT,ev.event.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_UID,ev.uid.getBytes());
		row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COMPANY,ev.company.getBytes());

		if( ev.agent != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_AGENT,ev.agent.getBytes());
		}
		if( ev.gid != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GID,ev.gid.getBytes());
		}
		if( ev.ctx != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_CTX,ev.ctx.getBytes());
		}
		if( ev.ip != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_IP,ev.ip.getBytes());
		}
		if( ev.device != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DEVICE,ev.device.getBytes());
		}
		if( ev.devid != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DEVICE_ID,ev.devid.getBytes());
		}
		if( ev.version != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_VERSION,ev.version.getBytes());
		}
		if( ev.imsi != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_IMSI,ev.imsi.getBytes());
		}
		if( ev.expect_payload != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_EXPECT_PAYLOAD,ev.expect_payload.getBytes());
		}
		if( ev.target != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TARGET,ev.target.getBytes());
		}
		if( ev.target_got != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TARGET_GOT,ev.target_got.getBytes());
		}
		if( ev.target_dent != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TARGET_DENT,ev.target_dent.getBytes());
		}
		if( ev.count != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COUNT,ev.count.getBytes());
		}
		if( ev.sw != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SW,ev.sw.getBytes());
		}
		if( ev.value != null ) {
			row.add(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_VALUE,ev.value.getBytes());
		}
		return row;
	}
}

