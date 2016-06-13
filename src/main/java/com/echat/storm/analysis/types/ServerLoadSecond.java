package com.echat.storm.analysis.types;

public class ServerLoadSecond {
	public final String			server;
	public final long			bucket;
	public UserLoadSecond		userLoad;
	public GroupLoadReport		groupLoad;
	public SpeakLoadSecond		speakLoad;

	public ServerLoadSecond(final String s,long b) {
		server = s;
		bucket = b;
		userLoad = new UserLoadSecond();
		groupLoad = new GroupLoadReport();
		speakLoad = new SpeakLoadSecond();
	}
}

