package com.imap4j.hbase.hbase;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 15, 2009
 * Time: 10:23:03 AM
 */
public class HValue {

    private boolean valueset = false;
    private Object currentValue = null;
    private Map<Long, Object> versionMap = null;

    public Object getCurrentValue() {
        return this.currentValue;
    }

    public void setCurrentValue(final Object currentValue) {
        this.valueset = true;
        this.currentValue = currentValue;
    }

    public Map<Long, Object> getVersionMap() {
        return this.versionMap;
    }

    public void setVersionMap(final Object versionMap) {
        this.versionMap = (Map<Long, Object>)versionMap;
    }
}
