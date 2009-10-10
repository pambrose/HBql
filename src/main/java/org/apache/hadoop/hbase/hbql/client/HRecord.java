package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.Serializable;
import java.util.Map;

public class HRecord implements Serializable {

    private HBaseSchema schema = null;
    private long timestamp = System.currentTimeMillis();

    private final Map<String, HValue> values = Maps.newHashMap();

    public HRecord(final HBaseSchema schema) {
        this.setSchema(schema);
    }

    public HRecord(final String tablename) throws HBqlException {
        this(HBaseSchema.findSchema(tablename));
    }

    HBaseSchema getSchema() {
        return this.schema;
    }

    private Map<String, HValue> getValues() {
        return this.values;
    }

    public void setSchema(final HBaseSchema schema) {
        this.schema = schema;
    }

    private HValue addValue(final String name, final boolean inSchema) throws HBqlException {

        if (inSchema && !this.getSchema().constainsVariableName(name))
            throw new HBqlException("Invalid variable name " + this.getSchema().getTableName() + "." + name);

        final HValue val = new HValue(inSchema);
        this.getValues().put(name, val);
        return val;
    }

    private HValue getValue(final String name) {
        return this.getValues().get(name);
    }

    private boolean containsName(final String name) {
        return this.getValues().containsKey(name);
    }

    private HValue getHValue(final String name, final boolean inSchema) throws HBqlException {
        HValue hvalue = this.getValue(name);
        if (hvalue == null)
            hvalue = this.addValue(name, inSchema);
        return hvalue;
    }

    private HValue getHValue(final String name) {

        // First try the name given.
        // If that doesn't work, then try alias and qualified (one hasn't been tried yet)
        if (this.containsName(name))
            return this.getValue(name);

        // Look up by both variable name and qualified name
        final ColumnAttrib attrib = this.getSchema().getAttribByVariableName(name);

        if (attrib != null) {
            final String aliasName = attrib.getAliasName();
            if (aliasName.equals(name) && this.containsName(aliasName))
                return this.getValue(aliasName);

            final String qualifiedName = attrib.getFamilyQualifiedName();
            if (!qualifiedName.equals(name) && this.containsName(qualifiedName))
                return this.getValue(qualifiedName);
        }

        return null;
    }

    public Object getCurrentValue(final String name) {
        final HValue hvalue = this.getHValue(name);
        return (hvalue != null) ? hvalue.getCurrentValue() : null;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public void setCurrentValue(final String name, final Object val) throws HBqlException {
        this.setCurrentValue(name, val, true);
    }

    public void setCurrentValue(final String name, final Object val, final boolean inSchema) throws HBqlException {
        this.setCurrentValue(name, this.getTimestamp(), val, inSchema);
    }

    public void setCurrentValue(final String name,
                                final long timestamp,
                                final Object val,
                                final boolean inSchema) throws HBqlException {

        final HValue hvalue = this.getHValue(name, inSchema);
        hvalue.setCurrentValue(timestamp, val);
    }

    public void setKeysAsColumnsValue(final String name,
                                      final long timestamp,
                                      final String mapKey,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {

        HValue hvalue = this.getHValue(name);

        Map<String, HValue> mapval;

        if (hvalue == null) {
            hvalue = this.addValue(name, inSchema);
            mapval = Maps.newHashMap();
            hvalue.setCurrentValue(timestamp, mapval);
        }
        else {
            mapval = (Map<String, HValue>)hvalue.getCurrentValue();
        }

        final HValue newval = new HValue(inSchema);
        newval.setCurrentValue(timestamp, val);
        mapval.put(mapKey, newval);
    }

    public Map<Long, Object> getVersionedValueMap(final String name) {
        final HValue hvalue = this.getHValue(name);
        return (hvalue != null) ? hvalue.getVersionMap() : null;
    }

    public void setVersionedValueMap(final String name,
                                     final Map<Long, Object> val,
                                     final boolean inSchema) throws HBqlException {
        final HValue hvalue = this.getHValue(name, inSchema);
        hvalue.setVersionMap(val);
    }

    public void setVersionedValue(final String name,
                                  final long timestamp,
                                  final Object val,
                                  final boolean inSchema) throws HBqlException {

        final HValue hvalue = this.getHValue(name, inSchema);
        hvalue.getVersionMap().put(timestamp, val);
    }

    public void setCurrentValue(final String family,
                                final String column,
                                final long timestamp,
                                final Object val) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(family, column);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + family + ":" + column);
        this.setCurrentValue(attrib.getAliasName(), timestamp, val, true);
    }

    public void setVersionedValue(final String family,
                                  final String column,
                                  final long timestamp,
                                  final Object val,
                                  final boolean inSchema) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(family, column);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + family + ":" + column);
        this.setVersionedValue(attrib.getColumnName(), timestamp, val, inSchema);
    }

    public boolean isCurrentValueSet(final ColumnAttrib attrib) {
        final HValue hvalue = this.getHValue(attrib.getAliasName());
        return hvalue != null && hvalue.isCurrentValueSet();
    }

    public void clear() {
        this.getValues().clear();
    }
}
