package com.echat.storm.analysis.types;

import java.io.Serializable;

public class RedisConfig implements Serializable {
	public String host = "localhost";
	public int port = 6379;
	public int timeout = 3000;

	public static class Builder {
		private RedisConfig _conf = new RedisConfig();

		public RedisConfig build() {
			return _conf;
		}

		public Builder setHost(String host) {
			_conf.host = host;
			return this;
		}
		public Builder setPort(int port) {
			_conf.port = port;
			return this;
		}
		public Builder setTimeout(int timeout) {
			_conf.timeout = timeout;
			return this;
		}
	}

	public static RedisConfig defaultConfig() {
		return new RedisConfig.Builder().setHost("192.168.1.181").setPort(6379).build();
	}
}


