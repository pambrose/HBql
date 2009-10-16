package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HValue;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.Map;
import java.util.NavigableMap;

public abstract class ValueMap<T> extends HValue {

    private Map<String, CurrentAndVersionValue<T>> currentAndVersionMap = Maps.newHashMap();

    protected ValueMap(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name);
    }

    public Map<String, CurrentAndVersionValue<T>> getCurrentAndVersionMap() {
        return this.currentAndVersionMap;
    }

    public T getCurrentMapValue(final String name) throws HBqlException {
        return this.getValueFromMapWithDefault(name).getValue();
    }

    public CurrentAndVersionValue<T> getValueFromMapWithDefault(final String mapKey) throws HBqlException {
        CurrentAndVersionValue<T> hvalue = this.getCurrentAndVersionMap().get(mapKey);
        if (hvalue == null) {
            hvalue = new CurrentAndVersionValue<T>(null, null);
            this.getCurrentAndVersionMap().put(mapKey, hvalue);
        }
        return hvalue;
    }

    public void setCurrentValueMap(final long timestamp, final String mapKey, final T val) throws HBqlException {
        this.getValueFromMapWithDefault(mapKey).setCurrentValue(timestamp, val);
    }

    public Map<Long, T> getVersionMap(final String name, final boolean createIfNull) throws HBqlException {
        return this.getValueFromMapWithDefault(name).getVersionMap(createIfNull);
    }

    public void setVersionMap(final String name, final NavigableMap<Long, T> val) throws HBqlException {
        this.getValueFromMapWithDefault(name).setVersionMap(val);
    }

    public void setVersionValue(final String mapKey, final Long ts, final T val) throws HBqlException {
        this.getValueFromMapWithDefault(mapKey).setVersionValue(ts, val);
    }

    public boolean isCurrentValueSet(final String mapKey) throws HBqlException {
        return this.getValueFromMapWithDefault(mapKey).isValueSet();
    }
}