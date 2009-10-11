package org.apache.hadoop.hbase.hbql.client;

import java.util.Map;
import java.util.TreeMap;

public class ObjectHValue extends HValue {

    private boolean currentValueSet = false;
    private Object currentValue = null;
    private long currentValueTimestamp = -1;
    private Map<Long, Object> versionMap = null;

    public Object getCurrentValue() {
        return this.currentValue;
    }

    public void setCurrentValue(final long timestamp, final Object val) {
        if (timestamp >= this.currentValueTimestamp) {
            this.currentValueSet = true;
            this.currentValueTimestamp = timestamp;
            this.currentValue = val;
        }
    }

    public Map<Long, Object> getVersionMap() {

        if (this.versionMap != null)
            return this.versionMap;

        synchronized (this) {
            if (this.versionMap == null)
                this.versionMap = new TreeMap<Long, Object>();

            return this.versionMap;
        }
    }

    public void setVersionValue(final Long ts, final Object val) {
        this.getVersionMap().put(ts, val);
    }

    public void setVersionMap(final Map<Long, Object> versionMap) {
        this.versionMap = versionMap;
    }

    public boolean isCurrentValueSet() {
        return currentValueSet;
    }
}