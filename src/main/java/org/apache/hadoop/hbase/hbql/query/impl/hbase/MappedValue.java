package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HValue;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.Map;

public abstract class MappedValue<T> implements HValue {

    private Map<String, ValueImpl<T>> keysAsColumnMap = Maps.newHashMap();

    public Object getCurrentValue(final String name) {
        return this.getHValue(name).getCurrentValue();
    }

    private Map<String, ValueImpl<T>> getKeysAsColumnMap() {
        return this.keysAsColumnMap;
    }

    private ValueImpl<T> getHValue(final String mapKey) {
        ValueImpl<T> hvalue = this.getKeysAsColumnMap().get(mapKey);
        if (hvalue == null) {
            hvalue = new ValueImpl<T>();
            this.getKeysAsColumnMap().put(mapKey, hvalue);
        }
        return hvalue;
    }

    public void setCurrentValue(final long timestamp, final String mapKey, final T val) {
        this.getHValue(mapKey).setCurrentValue(timestamp, val);
    }

    public Map<Long, T> getVersionMap(final String name) {
        return this.getHValue(name).getVersionMap();
    }

    public void setVersionMap(final String name, final Map<Long, T> val) {
        this.getHValue(name).setVersionMap(val);
    }

    public void setVersionValue(final String mapKey, final Long ts, final T val) {
        this.getHValue(mapKey).setVersionValue(ts, val);
    }

    public boolean isCurrentValueSet(final String mapKey) {
        return this.getHValue(mapKey).isCurrentValueSet();
    }
}