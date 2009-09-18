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

    private final Map<String, HValue> values = Maps.newHashMap();

    public HRecord(final HBaseSchema schema) {
        this.schema = schema;
    }

    private HBaseSchema getSchema() {
        return this.schema;
    }

    private Map<String, HValue> getValues() {
        return this.values;
    }

    public void setSchema(final HBaseSchema schema) {
        this.schema = schema;
    }

    private HValue addValue(final String name) {
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

    private HValue getHValue(final String name) throws HPersistException {
        // First try the name given.
        // If that doesn't work, then try variable and qualified (one hasn't been tried yet)
        if (this.containsName(name))
            return this.getValue(name);

        if (this.getSchema().constainsVariableName(name)) {
            // Look up by both variable name and qualified name
            final VariableAttrib attrib = this.getSchema().getVariableAttribByVariableName(name);

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
        try {
            final HValue hvalue = this.getHValue(name);
            return (hvalue != null) ? hvalue.getCurrentValue() : null;
        }
        catch (HPersistException e) {
            // This should not be executed
            return null;
        }
    }

    public void setCurrentValueByVariableName(final String name, final long timestamp, final Object val) {
        final HValue hvalue = (!this.containsName(name)) ? this.addValue(name) : this.getValue(name);
        hvalue.setCurrentValue(timestamp, val);
    }

    public Map<Long, Object> getVersionedValueMap(final String name) {
        try {
            final HValue hvalue = this.getHValue(name);
            return (hvalue != null) ? hvalue.getVersionMap() : null;
        }
        catch (HPersistException e) {
            // This should not be executed
            return null;
        }
    }

    public void setVersionedValueMapByVariableName(final String name, final Map<Long, Object> val) {
        this.getValue(name).setVersionMap(val);
    }

    public void setVersionedValueByVariableName(final String name, final long timestamp, final Object val) {
        this.getValue(name).getVersionMap().put(timestamp, val);
    }

    public void setCurrentValueByFamilyQualifiedName(final String family,
                                                     final String column,
                                                     final long timestamp,
                                                     final Object val) {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribByFamilyQualifiedColumnName(family, column);
        this.setCurrentValueByVariableName(attrib.getVariableName(), timestamp, val);
    }

    public void setVersionedValueByFamilyQualifiedName(final String family,
                                                       final String column,
                                                       final long timestamp,
                                                       final Object val) {
        final ColumnAttrib attrib = this.getSchema().getColumnAttribByFamilyQualifiedColumnName(family, column);
        this.setVersionedValueByVariableName(attrib.getVariableName(), timestamp, val);
    }

    public void clear() {
        this.getValues().clear();
    }
}
