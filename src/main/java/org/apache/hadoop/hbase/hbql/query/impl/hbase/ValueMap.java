package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HValue;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.Map;
import java.util.NavigableMap;

public abstract class ValueMap<T> extends HValue {

    private Map<String, CurrentAndVersionValue<T>> valueMap = Maps.newHashMap();

    protected ValueMap(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name);
    }

    public Map<String, CurrentAndVersionValue<T>> getValueMap() {
        return this.valueMap;
    }

    public T getCurrentMapValue(final String name) throws HBqlException {
        return this.getValueFromMapWithDefault(name).getValue();
    }

    public CurrentAndVersionValue<T> getValueFromMapWithDefault(final String mapKey) throws HBqlException {
        CurrentAndVersionValue<T> hvalue = this.getValueMap().get(mapKey);
        if (hvalue == null) {
            hvalue = new CurrentAndVersionValue<T>(null, null);
            this.getValueMap().put(mapKey, hvalue);
        }
        return hvalue;
    }

    public void setMapValue(final long timestamp, final String mapKey, final T val) throws HBqlException {
        this.getValueFromMapWithDefault(mapKey).setCurrentValue(timestamp, val);
    }

    public Map<Long, T> getVersionMap(final String name) throws HBqlException {
        return this.getValueFromMapWithDefault(name).getVersionMap();
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