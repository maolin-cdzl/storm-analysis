package com.echat.storm.analysis.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUHashMap<A, B> extends LinkedHashMap<A, B> {
    private final int _maxSize;

    public LRUHashMap(int maxSize) {
        super(maxSize + 1, 0.75f, true);
        _maxSize = maxSize;
    }
    
    @Override
    protected boolean removeEldestEntry(final Map.Entry<A, B> eldest) {
        return size() > _maxSize;
    }
}

