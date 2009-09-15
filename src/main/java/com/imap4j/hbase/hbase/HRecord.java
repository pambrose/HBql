package com.imap4j.hbase.hbase;

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

    public Object getCurrentValueByVariableName(final String name) {
        return this.values.get(name).getCurrentValue();
    }

    public void setCurrentValueByVariableName(final String name, final Object val) {
        this.setCurrentValue(name, val);
    }

    public Object getVersionedValueMapByVariableName(final String name) {
        return this.values.get(name).getVersionMap();
    }

    public void setVersionedValueMapByVariableName(final String name, final Object val) {
        this.values.get(name).setVersionMap(val);
    }

    private void setCurrentValue(final String name, final Object val) {
        final HValue hvalue = (!this.values.containsKey(name)) ? this.addValue(name) : this.values.get(name);
        hvalue.setCurrentValue(val);
    }

    private HValue addValue(final String name) {
        final HValue val = new HValue();
        this.values.put(name, val);
        return val;
    }

    /*
    public Object getCurrentValueByFamilyQualifiedName(final String name) {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribByFamilyQualifiedColumnName(name);
        return this.getCurrentValueByVariableName(attrib.getVariableName());
    }

    public void setCurrentValueByFamilyQualifiedName(final String name, final Object val) {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribByFamilyQualifiedColumnName(name);
        this.setCurrentValueByVariableName(attrib.getVariableName(), val);
    }

    public Object getVersionedValueByFamilyQualifiedName(final String name) {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribByFamilyQualifiedColumnName(name);
        return this.getVersionedValueMapByVariableName(attrib.getVariableName());
    }

    public void setVersionedValueByFamilyQualifiedName(final String name, final Object val) {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribByFamilyQualifiedColumnName(name);
        this.setVersionedValueMapByVariableName(attrib.getVariableName(), val);
    }
    */

    public void clear() {
        this.values.clear();
    }
}
