package com.imap4j.hbase.hbase;

import com.imap4j.hbase.hbql.schema.ColumnAttrib;
import com.imap4j.hbase.hbql.schema.HBaseSchema;
import com.imap4j.hbase.util.Maps;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 12, 2009
 * Time: 11:06:07 PM
 */
public class HRecord implements Serializable {

    private HBaseSchema schema = null;

    private final Map<String, HValue> values = Maps.newHashMap();

    public HRecord(final HBaseSchema schema) {
        this.schema = schema;
    }

    private HBaseSchema getSchema() {
        return this.schema;
    }

    public void setSchema(final HBaseSchema schema) {
        this.schema = schema;
    }

    private HValue addValue(final String name) {
        final HValue val = new HValue();
        this.values.put(name, val);
        return val;
    }

    private HValue getValue(final String name) {
        return this.values.get(name);
    }

    private boolean isDefined(final String name) {
        return this.values.containsKey(name);
    }

    public Object getCurrentValueByVariableName(final String name) throws HPersistException {
        if (this.values.containsKey(name))
            return this.getValue(name).getCurrentValue();
        else
            throw new HPersistException("No value set for variable " + name);
    }

    public void setCurrentValueByVariableName(final String name, final long timestamp, final Object val) {
        final HValue hvalue = (!this.isDefined(name)) ? this.addValue(name) : this.getValue(name);
        hvalue.setCurrentValue(timestamp, val);
    }

    public Object getVersionedValueMapByVariableName(final String name) {
        if (this.isDefined(name))
            return this.getValue(name).getVersionMap();
        else
            return null;
    }

    public void setVersionedValueMapByVariableName(final String name, final Map<Long, Object> val) {
        this.getValue(name).setVersionMap(val);
    }

    public void setVersionedValueByVariableName(final String name, final long timestamp, final Object val) {
        this.getValue(name).getVersionMap().put(timestamp, val);
    }

    public void setCurrentValueByFamilyQualifiedName(final String name, final long timestamp, final Object val) {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribByFamilyQualifiedColumnName(name);
        this.setCurrentValueByVariableName(attrib.getVariableName(), timestamp, val);
    }

    public void setVersionedValueByFamilyQualifiedName(final String name, final long timestamp, final Object val) {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribByFamilyQualifiedColumnName(name);
        this.setVersionedValueByVariableName(attrib.getVariableName(), timestamp, val);
    }

    public void clear() {
        this.values.clear();
    }
}
