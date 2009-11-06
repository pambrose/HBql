package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.contrib.hbql.client.FamilyDefaultValueMap;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.client.Record;
import org.apache.hadoop.hbase.contrib.hbql.client.Value;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.contrib.hbql.schema.DefinedSchema;
import org.apache.hadoop.hbase.contrib.hbql.schema.HBaseSchema;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class RecordImpl implements Serializable, Record {

    private DefinedSchema schema = null;
    private long timestamp = System.currentTimeMillis();

    private volatile ElementMap<ColumnValue> columnValues = null;
    private volatile ElementMap<FamilyDefaultValueMap> familyDefaultElements = null;

    public RecordImpl(final DefinedSchema schema) {
        this.setSchema(schema);
    }

    public HBaseSchema getSchema() {
        return this.schema;
    }

    public void setSchema(final DefinedSchema schema) {
        this.schema = schema;
    }

    private ElementMap<ColumnValue> getColumnValues() {
        if (this.columnValues == null)
            synchronized (this) {
                if (this.columnValues == null)
                    this.columnValues = new ElementMap<ColumnValue>(this);
            }
        return this.columnValues;
    }

    private ElementMap<FamilyDefaultValueMap> getFamilyDefaultElements() {
        if (this.familyDefaultElements == null)
            synchronized (this) {
                if (this.familyDefaultElements == null)
                    this.familyDefaultElements = new ElementMap<FamilyDefaultValueMap>(this);
            }
        return this.familyDefaultElements;
    }

    public void addElement(final String name, final Value value) throws HBqlException {
        if (value instanceof ColumnValue)
            this.getColumnValues().addElement(name, (ColumnValue)value);
        else if (value instanceof FamilyDefaultValueMap)
            this.getFamilyDefaultElements().addElement(name, (FamilyDefaultValueMap)value);
        else
            throw new InternalErrorException(value.getClass().getName());
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void clearValues() {
        this.getColumnValues().clear();
        this.getFamilyDefaultElements().clear();
    }

    // Simple get routines
    public ColumnValue getColumnValue(final String name, final boolean inSchema) throws HBqlException {
        final ColumnValue value = this.getColumnValues().findElement(name);
        if (value != null) {
            return value;
        }
        else {
            if (inSchema && !this.getSchema().containsVariableName(name))
                throw new HBqlException("Invalid variable name " + this.getSchema().getSchemaName() + "." + name);
            return new ColumnValue(this, name);
        }
    }

    private FamilyDefaultValueMap getFamilyDefaultValueMap(final String name,
                                                           final boolean createNewIfMissing) throws HBqlException {
        final FamilyDefaultValueMap value = this.getFamilyDefaultElements().findElement(name);
        if (value != null) {
            return value;
        }
        else {
            if (createNewIfMissing)
                return new FamilyDefaultValueMap(this, name);
            else
                return null;
        }
    }

    // Current Object values
    public void setCurrentValue(final String family,
                                final String column,
                                final long timestamp,
                                final Object val) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(family, column);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + family + ":" + column);
        this.setCurrentValue(attrib.getAliasName(), timestamp, val, true);
    }

    public boolean isCurrentValueSet(final ColumnAttrib attrib) {
        final ColumnValue columnValue = this.getColumnValues().findElement(attrib.getAliasName());
        return columnValue != null && columnValue.isValueSet();
    }


    public void setCurrentValue(final String name,
                                final long timestamp,
                                final Object val,
                                final boolean inSchema) throws HBqlException {
        this.getColumnValue(name, inSchema).setCurrentValue(timestamp, val);
    }

    public void setVersionValue(final String familyName,
                                final String columnName,
                                final long timestamp,
                                final Object val,
                                final boolean inSchema) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(familyName, columnName);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + familyName + ":" + columnName);

        this.getColumnValue(attrib.getColumnName(), inSchema).getVersionMap(true).put(timestamp, val);
    }

    public void setFamilyDefaultCurrentValue(final String familyName,
                                             final String name,
                                             final long timestamp,
                                             final byte[] val) throws HBqlException {
        this.getFamilyDefaultValueMap(familyName + ":*", true).setCurrentValueMap(timestamp, name, val);
    }

    public void setFamilyDefaultVersionMap(final String familyName,
                                           final String name,
                                           final NavigableMap<Long, byte[]> val) throws HBqlException {
        this.getFamilyDefaultValueMap(familyName + ":*", true).setVersionMap(name, val);
    }

    public void reset() {
        this.columnValues = null;
        this.familyDefaultElements = null;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public void setCurrentValue(final String name, final Object val) throws HBqlException {
        this.setCurrentValue(name, this.getTimestamp(), val, true);
    }

    public Object getCurrentValue(final String name) throws HBqlException {
        final ColumnValue columnValue = this.getColumnValues().findElement(name);
        if (columnValue != null) {
            final Object retval = columnValue.getValue();
            if (retval != null)
                return retval;
        }

        // Return default value if it exists
        final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);
        return (attrib != null) ? attrib.getDefaultValue() : null;
    }

    public Set<String> getColumnNameList() throws HBqlException {
        return this.getColumnValues().getMap().keySet();
    }

    public Map<Long, Object> getVersionMap(final String name) {
        final ColumnValue value = this.getColumnValues().findElement(name);
        return (value != null) ? value.getVersionMap(true) : null;
    }

    public Map<String, byte[]> getFamilyDefaultValueMap(final String name) throws HBqlException {

        final FamilyDefaultValueMap value = this.getFamilyDefaultValueMap(name, false);
        if (value == null)
            return null;

        final Map<String, byte[]> retval = Maps.newHashMap();
        for (final String key : value.getCurrentAndVersionMap().keySet())
            retval.put(key, value.getCurrentAndVersionMap().get(key).getValue());
        return retval;
    }

    public Map<String, NavigableMap<Long, byte[]>> getFamilyDefaultVersionMap(final String name) throws HBqlException {

        final FamilyDefaultValueMap value = this.getFamilyDefaultValueMap(name, false);
        if (value == null)
            return null;

        final Map<String, NavigableMap<Long, byte[]>> retval = Maps.newHashMap();
        for (final String key : value.getCurrentAndVersionMap().keySet())
            retval.put(key, value.getCurrentAndVersionMap().get(key).getVersionMap(true));
        return retval;
    }
}
