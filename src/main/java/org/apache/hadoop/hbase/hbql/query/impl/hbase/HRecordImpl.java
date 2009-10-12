package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.FamilyDefaultHValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.HValue;
import org.apache.hadoop.hbase.hbql.client.KeysAsColumnsHValue;
import org.apache.hadoop.hbase.hbql.client.ObjectHValue;
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

    public HRecordImpl(final String tablename) throws HBqlException {
        this(HBaseSchema.findSchema(tablename));
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

    public void clear() {
        this.getValues().clear();
    }

    // Add element routines
    private ObjectHValue addObjectHValue(final String name, final boolean inSchema) throws HBqlException {

        if (inSchema && !this.getSchema().constainsVariableName(name))
            throw new HBqlException("Invalid variable name " + this.getSchema().getTableName() + "." + name);

        final ObjectHValue val = new ObjectHValue();
        this.getValues().put(name, val);
        return val;
    }

    private KeysAsColumnsHValue addKeysAsColumnsHValue(final String name, final boolean inSchema) throws HBqlException {

        if (inSchema && !this.getSchema().constainsVariableName(name))
            throw new HBqlException("Invalid variable name " + this.getSchema().getTableName() + "." + name);

        final KeysAsColumnsHValue val = new KeysAsColumnsHValue();
        this.getValues().put(name, val);
        return val;
    }

    private FamilyDefaultHValue addFamilyDefaultHValue(final String name) throws HBqlException {
        final FamilyDefaultHValue val = new FamilyDefaultHValue();
        this.getValues().put(name, val);
        return val;
    }

    // Simple get routines
    public ObjectHValue getObjectHValue(final String name, final boolean inSchema) throws HBqlException {
        final ObjectHValue hvalue = this.getObjectHValue(name);
        return (hvalue != null) ? hvalue : this.addObjectHValue(name, inSchema);
    }

    public KeysAsColumnsHValue getKeysAsColumnsHValue(final String name, final boolean inSchema) throws HBqlException {
        final KeysAsColumnsHValue hvalue = this.getKeysAsColumnsHValue(name);
        return (hvalue != null) ? hvalue : this.addKeysAsColumnsHValue(name, inSchema);
    }

    public FamilyDefaultHValue getFamilyDefaultHValue(final String name) throws HBqlException {
        final FamilyDefaultHValue hvalue = this.getFamilyDefaultHValue2(name);
        return (hvalue != null) ? hvalue : this.addFamilyDefaultHValue(name);
    }

    // Primitive get routines
    private ObjectHValue getObjectHValue(final String name) throws HBqlException {
        final HValue hvalue = this.getHValue(name);
        if (hvalue instanceof ObjectHValue)
            return (ObjectHValue)hvalue;
        else
            throw new HBqlException("Not a ObjectHValue value");
    }

    private KeysAsColumnsHValue getKeysAsColumnsHValue(final String name) throws HBqlException {
        final HValue hvalue = this.getHValue(name);
        if (hvalue instanceof KeysAsColumnsHValue)
            return (KeysAsColumnsHValue)hvalue;
        else
            throw new HBqlException("Not a KeysAsColumnsHValue value");
    }

    private FamilyDefaultHValue getFamilyDefaultHValue2(final String name) throws HBqlException {
        final HValue hvalue = this.getHValue(name);
        if (hvalue instanceof FamilyDefaultHValue)
            return (FamilyDefaultHValue)hvalue;
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
        final ObjectHValue hvalue = this.getObjectHValue(attrib.getAliasName());
        return hvalue != null && hvalue.isCurrentValueSet();
    }

    public Object getObjectCurrentValue(final String name) throws HBqlException {
        final ObjectHValue hvalue = this.getObjectHValue(name);
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
        final ObjectHValue hvalue = this.getObjectHValue(name, inSchema);
        hvalue.setCurrentValue(timestamp, val);
    }

    // Access to version maps
    public Map<Long, Object> getObjectVersionValueMap(final String name) throws HBqlException {
        final ObjectHValue hvalue = this.getObjectHValue(name);
        return (hvalue != null) ? hvalue.getVersionMap() : null;
    }

    public Map<Long, Object> getKeysAsColumnsVersionValueMap(final String name, final String mapKey) throws HBqlException {
        final KeysAsColumnsHValue hvalue = this.getKeysAsColumnsHValue(name);
        return (hvalue != null) ? hvalue.getVersionMap(mapKey) : null;
    }

    public Map<Long, byte[]> getFamilyDefaultVersionMap(final String familyName, final String name) throws HBqlException {
        final FamilyDefaultHValue hvalue = this.getFamilyDefaultHValue(familyName);
        return (hvalue != null) ? hvalue.getVersionMap(name) : null;
    }

    // Version Object values
    public void setFamilyDefaultCurrentValue(final String familyName,
                                             final String name,
                                             final long timestamp,
                                             final byte[] val) throws HBqlException {
        final FamilyDefaultHValue hvalue = this.getFamilyDefaultHValue(familyName);
        hvalue.setCurrentValue(timestamp, name, val);
    }

    public void setObjectVersionValue(final String name,
                                      final long timestamp,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {
        final ObjectHValue hvalue = this.getObjectHValue(name, inSchema);
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
        final KeysAsColumnsHValue hvalue = this.getKeysAsColumnsHValue(name, inSchema);
        hvalue.setCurrentValue(timestamp, mapKey, val);
    }

    public void setKeysAsColumnsVersionMap(final String familyName,
                                           final String name,
                                           final NavigableMap<Long, byte[]> val) throws HBqlException {
        final KeysAsColumnsHValue hvalue = this.getKeysAsColumnsHValue(familyName);
        hvalue.setVersionMap(name, (Map)val);
    }

    // FamilyDefault values
    public void setFamilyDefaultVersionMap(final String familyName,
                                           final String name,
                                           final NavigableMap<Long, byte[]> val) throws HBqlException {
        final FamilyDefaultHValue hvalue = this.getFamilyDefaultHValue(familyName);
        hvalue.setVersionMap(name, val);
    }
}
