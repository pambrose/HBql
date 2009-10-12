package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.FamilyDefaultValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;

public class HRecordImpl implements Serializable, HRecord {

    private HBaseSchema schema = null;
    private long timestamp = System.currentTimeMillis();

    private final Map<String, HValue> values = Maps.newHashMap();

    public HRecordImpl(final HBaseSchema schema) {
        this.setSchema(schema);
    }

    public HBaseSchema getSchema() {
        return this.schema;
    }

    private Map<String, HValue> getValues() {
        return this.values;
    }

    public void setSchema(final HBaseSchema schema) {
        this.schema = schema;
    }

    private HValue getValue(final String name) {
        return this.getValues().get(name);
    }

    private boolean containsName(final String name) {
        return this.getValues().containsKey(name);
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

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public void clearValues() {
        this.getValues().clear();
    }

    // Add element routines
    private ObjectValue addObjectHValue(final String name, final boolean inSchema) throws HBqlException {

        if (inSchema && !this.getSchema().constainsVariableName(name))
            throw new HBqlException("Invalid variable name " + this.getSchema().getTableName() + "." + name);

        final ObjectValue val = new ObjectValue();
        this.getValues().put(name, val);
        return val;
    }

    private KeysAsColumnsValue addKeysAsColumnsHValue(final String name, final boolean inSchema) throws HBqlException {

        if (inSchema && !this.getSchema().constainsVariableName(name))
            throw new HBqlException("Invalid variable name " + this.getSchema().getTableName() + "." + name);

        final KeysAsColumnsValue val = new KeysAsColumnsValue();
        this.getValues().put(name, val);
        return val;
    }

    private FamilyDefaultValue addFamilyDefaultHValue(final String name) throws HBqlException {
        final FamilyDefaultValue val = new FamilyDefaultValue();
        this.getValues().put(name, val);
        return val;
    }

    // Simple get routines
    public ObjectValue getObjectHValue(final String name, final boolean inSchema) throws HBqlException {
        final ObjectValue hvalue = this.fetchObjectHValue(name);
        return (hvalue != null) ? hvalue : this.addObjectHValue(name, inSchema);
    }

    public KeysAsColumnsValue getKeysAsColumnsHValue(final String name, final boolean inSchema) throws HBqlException {
        final KeysAsColumnsValue hvalue = this.fetchKeysAsColumnsHValue(name);
        return (hvalue != null) ? hvalue : this.addKeysAsColumnsHValue(name, inSchema);
    }

    public FamilyDefaultValue getFamilyDefaultHValue(final String name) throws HBqlException {
        final FamilyDefaultValue hvalue = this.fetchFamilyDefaultHValue(name);
        return (hvalue != null) ? hvalue : this.addFamilyDefaultHValue(name);
    }

    // Primitive get routines
    private ObjectValue fetchObjectHValue(final String name) throws HBqlException {
        final HValue hvalue = this.getHValue(name);
        if (hvalue == null)
            return null;
        else if (hvalue instanceof ObjectValue)
            return (ObjectValue)hvalue;
        else
            throw new HBqlException("Not an ObjectHValue value");
    }

    private KeysAsColumnsValue fetchKeysAsColumnsHValue(final String name) throws HBqlException {
        final HValue hvalue = this.getHValue(name);
        if (hvalue == null)
            return null;
        else if (hvalue instanceof KeysAsColumnsValue)
            return (KeysAsColumnsValue)hvalue;
        else
            throw new HBqlException("Not a KeysAsColumnsHValue value");
    }

    private FamilyDefaultValue fetchFamilyDefaultHValue(final String name) throws HBqlException {
        final HValue hvalue = this.getHValue(name);
        if (hvalue == null)
            return null;
        else if (hvalue instanceof FamilyDefaultValue)
            return (FamilyDefaultValue)hvalue;
        else
            throw new HBqlException("Not a FamilyDefaultHValue value");
    }

    // Current Object values
    public void setObjectCurrentValue(final String family,
                                      final String column,
                                      final long timestamp,
                                      final Object val) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(family, column);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + family + ":" + column);
        this.setObjectCurrentValue(attrib.getAliasName(), timestamp, val, true);
    }

    public boolean isObjectCurrentValueSet(final ColumnAttrib attrib) throws HBqlException {
        final ObjectValue hvalue = this.fetchObjectHValue(attrib.getAliasName());
        return hvalue != null && hvalue.isCurrentValueSet();
    }

    public Object getObjectCurrentValue(final String name) throws HBqlException {
        final ObjectValue hvalue = this.fetchObjectHValue(name);
        return (hvalue != null) ? hvalue.getCurrentValue() : null;
    }

    public void setObjectCurrentValue(final String name, final Object val) throws HBqlException {
        this.setObjectCurrentValue(name, val, true);
    }

    public void setObjectCurrentValue(final String name, final Object val, final boolean inSchema) throws HBqlException {
        this.setObjectCurrentValue(name, this.getTimestamp(), val, inSchema);
    }

    public void setObjectCurrentValue(final String name,
                                      final long timestamp,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {
        final ObjectValue hvalue = this.getObjectHValue(name, inSchema);
        hvalue.setCurrentValue(timestamp, val);
    }

    // Access to version maps
    public Map<Long, Object> getObjectVersionMap(final String name) throws HBqlException {
        final ObjectValue hvalue = this.fetchObjectHValue(name);
        return (hvalue != null) ? hvalue.getVersionMap() : null;
    }

    public Map<Long, Object> getKeysAsColumnsVersionMap(final String name, final String mapKey) throws HBqlException {
        final KeysAsColumnsValue hvalue = this.fetchKeysAsColumnsHValue(name);
        return (hvalue != null) ? hvalue.getVersionMap(mapKey) : null;
    }

    public Map<Long, byte[]> getFamilyDefaultVersionMap(final String familyName, final String name) throws HBqlException {
        final FamilyDefaultValue hvalue = this.getFamilyDefaultHValue(familyName);
        return (hvalue != null) ? hvalue.getVersionMap(name) : null;
    }

    // Version Object values
    public void setFamilyDefaultCurrentValue(final String familyName,
                                             final String name,
                                             final long timestamp,
                                             final byte[] val) throws HBqlException {
        final FamilyDefaultValue hvalue = this.getFamilyDefaultHValue(familyName);
        hvalue.setCurrentValue(timestamp, name, val);
    }

    public void setObjectVersionValue(final String name,
                                      final long timestamp,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {
        final ObjectValue hvalue = this.getObjectHValue(name, inSchema);
        hvalue.getVersionMap().put(timestamp, val);
    }

    public void setObjectVersionValue(final String family,
                                      final String column,
                                      final long timestamp,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(family, column);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + family + ":" + column);
        this.setObjectVersionValue(attrib.getColumnName(), timestamp, val, inSchema);
    }


    // Current KeysAsColumns values
    public void setKeysAsColumnsCurrentValue(final String name,
                                             final String mapKey,
                                             final long timestamp,
                                             final Object val,
                                             final boolean inSchema) throws HBqlException {
        final KeysAsColumnsValue hvalue = this.getKeysAsColumnsHValue(name, inSchema);
        hvalue.setCurrentValue(timestamp, mapKey, val);
    }

    public void setKeysAsColumnsVersionMap(final String familyName,
                                           final String name,
                                           final NavigableMap<Long, byte[]> val) throws HBqlException {
        final KeysAsColumnsValue hvalue = this.fetchKeysAsColumnsHValue(familyName);
        hvalue.setVersionMap(name, (Map)val);
    }

    // FamilyDefault values
    public void setFamilyDefaultVersionMap(final String familyName,
                                           final String name,
                                           final NavigableMap<Long, byte[]> val) throws HBqlException {
        final FamilyDefaultValue hvalue = this.getFamilyDefaultHValue(familyName);
        hvalue.setVersionMap(name, val);
    }
}
