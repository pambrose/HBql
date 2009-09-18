package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

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
    private long now = System.currentTimeMillis();

    private final Map<String, HValue> values = Maps.newHashMap();

    public HRecord() {
    }

    public HRecord(final String tablename) throws HPersistException {
        final HBaseSchema schema = HBaseSchema.findSchema(tablename);
        this.setSchema(schema);
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

    private HValue addValue(final String name) throws HPersistException {

        if (!this.getSchema().constainsVariableName(name))
            throw new HPersistException("Invalid variable name " + this.getSchema().getTableName() + "." + name);

        final HValue val = new HValue();
        this.getValues().put(name, val);
        return val;
    }

    private HValue getValue(final String name) {
        return this.getValues().get(name);
    }

    private boolean containsName(final String name) {
        return this.getValues().containsKey(name);
    }

    private HValue getHValue(final String name) {

        // First try the name given.
        // If that doesn't work, then try variable and qualified (one hasn't been tried yet)
        if (this.containsName(name))
            return this.getValue(name);

        // Look up by both variable name and qualified name
        final VariableAttrib attrib = this.getSchema().getVariableAttribByVariableName(name);

        if (attrib != null) {
            final String variableName = attrib.getVariableName();
            if (!variableName.equals(name) && containsName(variableName))
                return this.getValue(variableName);

            final String qualifiedName = attrib.getFamilyQualifiedName();
            if (!qualifiedName.equals(name) && containsName(qualifiedName))
                return this.getValue(qualifiedName);
        }

        return null;
    }

    public Object getCurrentValue(final String name) {
        final HValue hvalue = this.getHValue(name);
        return (hvalue != null) ? hvalue.getCurrentValue() : null;
    }

    public void setCurrentValue(final String name, final Object val) throws HPersistException {
        HValue hvalue = this.getHValue(name);

        if (hvalue == null)
            hvalue = this.addValue(name);

        hvalue.setCurrentValue(now, val);
    }

    public void setCurrentValue(final String name, final long timestamp, final Object val) throws HPersistException {
        HValue hvalue = this.getHValue(name);

        if (hvalue == null)
            hvalue = this.addValue(name);

        hvalue.setCurrentValue(timestamp, val);
    }

    public Map<Long, Object> getVersionedValueMap(final String name) {
        final HValue hvalue = this.getHValue(name);
        return (hvalue != null) ? hvalue.getVersionMap() : null;
    }

    public void setVersionedValueMap(final String name, final Map<Long, Object> val) {
        this.getValue(name).setVersionMap(val);
    }

    public void setVersionedValue(final String name, final long timestamp, final Object val) {
        this.getValue(name).getVersionMap().put(timestamp, val);
    }

    public void setCurrentValue(final String family,
                                final String column,
                                final long timestamp,
                                final Object val) throws HPersistException {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribFromFamilyQualifiedNameMap(family, column);
        if (attrib == null)
            throw new HPersistException("Invalid column name " + family + ":" + column);
        this.setCurrentValue(attrib.getVariableName(), timestamp, val);
    }

    public void setVersionedValue(final String family,
                                  final String column,
                                  final long timestamp,
                                  final Object val) throws HPersistException {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribFromFamilyQualifiedNameMap(family, column);
        if (attrib == null)
            throw new HPersistException("Invalid column name " + family + ":" + column);
        this.setVersionedValue(attrib.getVariableName(), timestamp, val);
    }

    public boolean isCurrentValueSet(final ColumnAttrib keyAttrib) {
        final HValue hvalue = this.getHValue(keyAttrib.getVariableName());
        return hvalue != null && hvalue.isCurrentValueSet();
    }

    public void clear() {
        this.getValues().clear();
    }
}
