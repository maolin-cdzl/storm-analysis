package com.echat.storm.analysis.types;

public class GroupLoadReport {
	public int				groups = 0;
	public int				temp_groups = 0;

	public GroupLoadReport merge(final GroupLoadReport other) {
		groups += other.groups;
		temp_groups += other.temp_groups;
		return this;
	}
}

