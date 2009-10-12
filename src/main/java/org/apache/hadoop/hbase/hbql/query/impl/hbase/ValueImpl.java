package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HValue;

import java.util.Map;
import java.util.TreeMap;

public class ValueImpl<T> implements HValue {

    private boolean currentValueSet = false;
    private T currentValue = null;
    private long currentValueTimestamp = -1;
    private Map<Long, T> versionMap = null;

    public T getCurrentValue() {
        return this.currentValue;
    }

    public void setCurrentValue(final long timestamp, final T val) {
        if (timestamp >= this.currentValueTimestamp) {
            this.currentValueSet = true;
            this.currentValueTimestamp = timestamp;
            this.currentValue = val;
        }
    }

    public Map<Long, T> getVersionMap() {

        if (this.versionMap != null)
            return this.versionMap;

        synchronized (this) {
            if (this.versionMap == null)
                this.versionMap = new TreeMap<Long, T>();

            return this.versionMap;
        }
    }

    public void setVersionValue(final Long ts, final T val) {
        this.getVersionMap().put(ts, val);
    }

    public void setVersionMap(final Map<Long, T> versionMap) {
        this.versionMap = versionMap;
    }

    public boolean isCurrentValueSet() {
        return currentValueSet;
    }
}