package com.echat.storm.analysis.types;

import java.io.Serializable;

public class HBaseConfig implements Serializable {
	public int poolSize = 10;

	public static HBaseConfig defaultConfig() {
		return new HBaseConfig();
	}
}


