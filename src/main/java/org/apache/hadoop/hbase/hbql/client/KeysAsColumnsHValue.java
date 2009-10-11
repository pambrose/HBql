package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.Map;

public class KeysAsColumnsHValue extends HValue {

    private Map<String, ObjectHValue> keysAsColumnMap = Maps.newHashMap();

    public Object getCurrentValue(final String name) {
        return this.getHValue(name).getCurrentValue();
    }

    private Map<String, ObjectHValue> getKeysAsColumnMap() {
        return this.keysAsColumnMap;
    }

    private ObjectHValue getHValue(final String mapKey) {
        ObjectHValue hvalue = this.getKeysAsColumnMap().get(mapKey);
        if (hvalue == null) {
            hvalue = new ObjectHValue();
            this.getKeysAsColumnMap().put(mapKey, hvalue);
        }
        return hvalue;
    }

    public void setCurrentValue(final long timestamp, final String mapKey, final Object val) {
        this.getHValue(mapKey).setCurrentValue(timestamp, val);
    }

    public Map<Long, Object> getVersionMap(final String mapKey) {
        return this.getHValue(mapKey).getVersionMap();
    }

    public void setVersionValue(final String mapKey, final Long ts, final Object val) {
        this.getHValue(mapKey).setVersionValue(ts, val);
    }

    public boolean isCurrentValueSet(final String mapKey) {
        return this.getHValue(mapKey).isCurrentValueSet();
    }
}