package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Value;

import java.util.NavigableMap;
import java.util.TreeMap;

public class CurrentAndVersionValue<T> extends Value {

    private boolean currentValueSet = false;
    private T currentValue = null;
    private long currentValueTimestamp = -1;
    private volatile NavigableMap<Long, T> versionMap = null;

    public CurrentAndVersionValue(final RecordImpl record, final String name) throws HBqlException {
        super(record, name);
    }

    public T getValue() {
        return this.currentValue;
    }

    public void setCurrentValue(final long timestamp, final T val) {
        if (timestamp >= this.currentValueTimestamp) {
            this.currentValueSet = true;
            this.currentValueTimestamp = timestamp;
            this.currentValue = val;
        }
    }

    public NavigableMap<Long, T> getVersionMap(final boolean createIfNull) {

        if (this.versionMap != null)
            return this.versionMap;

        if (!createIfNull)
            return null;

        synchronized (this) {
            if (this.versionMap == null)
                this.versionMap = new TreeMap<Long, T>();

            return this.versionMap;
        }
    }

    public void setVersionMap(final NavigableMap<Long, T> versionMap) {
        this.versionMap = versionMap;
    }

    public boolean isValueSet() {
        return currentValueSet;
    }
}