package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.Map;

public abstract class MappedHValue<T> implements HValue {

    private Map<String, ObjectHValue<T>> keysAsColumnMap = Maps.newHashMap();

    public Object getCurrentValue(final String name) {
        return this.getHValue(name).getCurrentValue();
    }

    private Map<String, ObjectHValue<T>> getKeysAsColumnMap() {
        return this.keysAsColumnMap;
    }

    private ObjectHValue<T> getHValue(final String mapKey) {
        ObjectHValue<T> hvalue = this.getKeysAsColumnMap().get(mapKey);
        if (hvalue == null) {
            hvalue = new ObjectHValue<T>();
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