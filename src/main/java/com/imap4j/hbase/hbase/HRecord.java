package com.imap4j.hbase.hbase;

import com.imap4j.hbase.hbql.schema.ColumnAttrib;
import com.imap4j.hbase.hbql.schema.HBaseSchema;
import com.imap4j.hbase.hbql.schema.VarDescAttrib;
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

    private final Map<String, VarDescAttrib> types = Maps.newHashMap();
    private final Map<String, Object> currentValues = Maps.newHashMap();
    private final Map<String, Object> versionValues = Maps.newHashMap();

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
        return this.currentValues.get(name);
    }

    public Object setCurrentValueByVariableName(final String name, final Object val) {
        return this.currentValues.put(name, val);
    }

    public Object getVersionedValueByVariableName(final String name) {
        return this.versionValues.get(name);
    }

    public void setVersionedValueByVariableName(final String name, final Object val) {
        this.versionValues.put(name, val);
    }

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
        return this.getVersionedValueByVariableName(attrib.getVariableName());
    }

    public void setVersionedValueByFamilyQualifiedName(final String name, final Object val) {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribByFamilyQualifiedColumnName(name);
        this.setVersionedValueByVariableName(attrib.getVariableName(), val);
    }

    public void clear() {
        this.currentValues.clear();
        this.versionValues.clear();
        this.types.clear();
    }
}
