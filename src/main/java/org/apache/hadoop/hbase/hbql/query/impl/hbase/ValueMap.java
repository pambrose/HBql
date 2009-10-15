package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HValue;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.Map;
import java.util.NavigableMap;

public abstract class ValueMap<T> extends HValue {

    private Map<String, ValueImpl<T>> valueMap = Maps.newHashMap();

    protected ValueMap(final HRecordImpl hrecord, final String name) {
        super(hrecord, name);
    }

    public Map<String, ValueImpl<T>> getValueMap() {
        return this.valueMap;
    }

    public T getCurrentValue(final String name) {
        return this.getHValue(name).getCurrentValue();
    }

    public ValueImpl<T> getHValue(final String mapKey) {
        ValueImpl<T> hvalue = this.getValueMap().get(mapKey);
        if (hvalue == null) {
            hvalue = new ValueImpl<T>(null, null);
            this.getValueMap().put(mapKey, hvalue);
        }
        return hvalue;
    }

    public void setCurrentValue(final long timestamp, final String mapKey, final T val) {
        this.getHValue(mapKey).setCurrentValue(timestamp, val);
    }

    public Map<Long, T> getVersionMap(final String name) {
        return this.getHValue(name).getVersionMap();
    }

    public void setVersionMap(final String name, final NavigableMap<Long, T> val) {
        this.getHValue(name).setVersionMap(val);
    }

    public void setVersionValue(final String mapKey, final Long ts, final T val) {
        this.getHValue(mapKey).setVersionValue(ts, val);
    }

    public boolean isCurrentValueSet(final String mapKey) {
        return this.getHValue(mapKey).isCurrentValueSet();
    }
}