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

    private HValue getValue(final String name) {
        return this.getValues().get(name);
    }

    private boolean containsName(final String name) {
        return this.getValues().containsKey(name);
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

    // Get version map routines
    public Map<Long, Object> getOrAddVersionValueMap(final String name) throws HBqlException {
        return this.getObjectHValue(name, true).getVersionMap();
    }

    public Map<Long, Object> getOrAddKeysAsColumnsVersionValueMap(final String name,
                                                                  final String mapKey) throws HBqlException {
        return this.getKeysAsColumnsHValue(name, true).getVersionMap(mapKey);
    }

    public Map<Long, Object> getOrAddFamilyDefaultVersionValueMap(final String name,
                                                                  final String mapKey) throws HBqlException {
        return this.getFamilyDefaultHValue(name).getVersionMap(mapKey);
    }


    // Simple get routines
    private ObjectHValue getObjectHValue(final String name, final boolean inSchema) throws HBqlException {

        final ObjectHValue hvalue = this.getObjectHValue(name);

        if (hvalue != null)
            return hvalue;
        else
            return this.addObjectHValue(name, inSchema);
    }

    private KeysAsColumnsHValue getKeysAsColumnsHValue(final String name, final boolean inSchema) throws HBqlException {

        final KeysAsColumnsHValue hvalue = this.getKeysAsColumnsValue(name);

        if (hvalue != null)
            return hvalue;
        else
            return this.addKeysAsColumnsHValue(name, inSchema);
    }

    private FamilyDefaultHValue getFamilyDefaultHValue(final String name) throws HBqlException {

        final FamilyDefaultHValue hvalue = this.getFamilyDefaultHValue2(name);

        if (hvalue != null)
            return hvalue;
        else
            return this.addFamilyDefaultHValue(name);
    }

    // Primitive get routines
    private ObjectHValue getObjectHValue(final String name) throws HBqlException {
        final HValue hvalue = this.getHValue(name);
        if (hvalue instanceof ObjectHValue)
            return (ObjectHValue)hvalue;
        else
            throw new HBqlException("Not a ObjectHValue value");
    }

    private KeysAsColumnsHValue getKeysAsColumnsValue(final String name) throws HBqlException {
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
    public void setCurrentObjectValue(final String family,
                                      final String column,
                                      final long timestamp,
                                      final Object val) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(family, column);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + family + ":" + column);
        this.setCurrentObjectValue(attrib.getAliasName(), timestamp, val, true);
    }

    public boolean isCurrentObjectValueSet(final ColumnAttrib attrib) throws HBqlException {
        final ObjectHValue hvalue = this.getObjectHValue(attrib.getAliasName());
        return hvalue != null && hvalue.isCurrentValueSet();
    }

    public Object getCurrentObjectValue(final String name) throws HBqlException {
        final ObjectHValue hvalue = this.getObjectHValue(name);
        return (hvalue != null) ? hvalue.getCurrentValue() : null;
    }

    public void setCurrentObjectValue(final String name, final Object val) throws HBqlException {
        this.setCurrentObjectValue(name, val, true);
    }

    public void setCurrentObjectValue(final String name, final Object val, final boolean inSchema) throws HBqlException {
        this.setCurrentObjectValue(name, this.getTimestamp(), val, inSchema);
    }

    public void setCurrentObjectValue(final String name,
                                      final long timestamp,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {
        final ObjectHValue hvalue = this.getObjectHValue(name, inSchema);
        hvalue.setCurrentValue(timestamp, val);
    }

    // Verion Object values
    public Map<Long, Object> getVersionObjectValueMap(final String name) throws HBqlException {
        final ObjectHValue hvalue = this.getObjectHValue(name);
        return (hvalue != null) ? hvalue.getVersionMap() : null;
    }

    public void setVersionObjectValueMap(final String name,
                                         final Map<Long, Object> val,
                                         final boolean inSchema) throws HBqlException {
        final ObjectHValue hvalue = this.getObjectHValue(name, inSchema);
        hvalue.setVersionMap(val);
    }

    public void setVersionObjectValue(final String name,
                                      final long timestamp,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {

        final ObjectHValue hvalue = this.getObjectHValue(name, inSchema);
        hvalue.getVersionMap().put(timestamp, val);
    }

    public void setVersionObjectValue(final String family,
                                      final String column,
                                      final long timestamp,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(family, column);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + family + ":" + column);
        this.setVersionObjectValue(attrib.getColumnName(), timestamp, val, inSchema);
    }


    // Current KeysAsColumns values
    public void setCurrentKeysAsColumnsValue(final String name,
                                             final String mapKey,
                                             final long timestamp,
                                             final Object val,
                                             final boolean inSchema) throws HBqlException {
        final KeysAsColumnsHValue hvalue = this.getKeysAsColumnsHValue(name, inSchema);
        hvalue.setCurrentValue(timestamp, mapKey, val);
    }

    public void setVersionKeysAsColumnsValue(final String name,
                                             final String mapKey,
                                             final long timestamp,
                                             final Object val,
                                             final boolean inSchema) throws HBqlException {
        final KeysAsColumnsHValue hvalue = this.getKeysAsColumnsHValue(name, inSchema);
        hvalue.setVersionValue(mapKey, timestamp, val);
    }

    public void setCurrentFamilyDefaultValue(final String name,
                                             final String mapKey,
                                             final long timestamp,
                                             final Object val) throws HBqlException {
        final FamilyDefaultHValue hvalue = this.getFamilyDefaultHValue(name);
        hvalue.setCurrentValue(timestamp, mapKey, val);
    }

    public void setVersionDefaultFamilyValue(final String name,
                                             final String mapKey,
                                             final long timestamp,
                                             final Object val) throws HBqlException {
        final FamilyDefaultHValue hvalue = this.getFamilyDefaultHValue(name);
        hvalue.setVersionValue(mapKey, timestamp, val);
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
}
