package com.imap4j.hbase.hbase;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 15, 2009
 * Time: 10:23:03 AM
 */
public class HValue {

    private boolean currentValueSet = false;
    private Object currentValue = null;
    private long currentValueTimestamp = -1;
    private Map<Long, Object> versionMap = null;

    public Object getCurrentValue() {
        return this.currentValue;
    }

    public void setCurrentValue(final long timestamp, final Object currentValue) {

        if (timestamp > this.currentValueTimestamp) {
            this.currentValueSet = true;
            this.currentValueTimestamp = timestamp;
            this.currentValue = currentValue;
        }
    }

    public Map<Long, Object> getVersionMap() {
        return this.versionMap;
    }

    public void setVersionMap(final Map<Long, Object> versionMap) {
        this.versionMap = versionMap;
    }

    public boolean isCurrentValueSet() {
        return currentValueSet;
    }
}
