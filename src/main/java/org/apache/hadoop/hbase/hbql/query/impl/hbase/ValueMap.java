package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HValue;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.Map;
import java.util.NavigableMap;

public abstract class ValueMap<T> extends HValue {

    private Map<String, CurrentAndVersionValue<T>> currentAndVersionMap = Maps.newHashMap();
    private final Class elementClazz;

    protected ValueMap(final HRecordImpl hrecord, final String name, final Class elementClazz) throws HBqlException {
        super(hrecord, name);
        this.elementClazz = elementClazz;
    }

    public Map<String, CurrentAndVersionValue<T>> getCurrentAndVersionMap() {
        return this.currentAndVersionMap;
    }

    private Class getElementClazz() {
        return this.elementClazz;
    }

    public T getCurrentMapValue(final String name, final boolean createIfNull) throws HBqlException {
        T retval = this.getValueFromMapWithDefault(name).getValue();

        if (retval != null || !createIfNull)
            return retval;

        if (this.getElementClazz() == null)
            throw new HBqlException("Internal error");

        T newVal = null;
        try {
            newVal = (T)this.getElementClazz().newInstance();
            this.setCurrentValueMap(0, name, newVal);
        }
        catch (InstantiationException e) {
            throw new HBqlException(e.getMessage());
        }
        catch (IllegalAccessException e) {
            throw new HBqlException(e.getMessage());
        }

        return newVal;
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