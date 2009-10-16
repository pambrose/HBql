package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.FamilyDefaultValueMap;
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

    private final ElementMap<ObjectValue> objectElements = new ElementMap<ObjectValue>(this);
    private final ElementMap<TypedKeysAsColumnsValueMap> keysAsColumnsElements = new ElementMap<TypedKeysAsColumnsValueMap>(this);
    private final ElementMap<FamilyDefaultValueMap> familyDefaultElements = new ElementMap<FamilyDefaultValueMap>(this);
    private final ElementMap<FamilyDefaultKeysAsColumnsValueMap> familyDefaultKeysAsColumnsElements = new ElementMap<FamilyDefaultKeysAsColumnsValueMap>(this);

    public HRecordImpl(final HBaseSchema schema) {
        this.setSchema(schema);
    }

    public HBaseSchema getSchema() {
        return this.schema;
    }

    public void setSchema(final HBaseSchema schema) {
        this.schema = schema;
    }

    public ElementMap<ObjectValue> getObjectElements() {
        return this.objectElements;
    }

    public ElementMap<TypedKeysAsColumnsValueMap> getKeysAsColumnsElements() {
        return this.keysAsColumnsElements;
    }

    public ElementMap<FamilyDefaultValueMap> getFamilyDefaultElements() {
        return this.familyDefaultElements;
    }

    public ElementMap<FamilyDefaultKeysAsColumnsValueMap> getFamilyDefaultKeysAsColumnsElements() {
        return this.familyDefaultKeysAsColumnsElements;
    }


    public void addElement(final String name, final HValue value) throws HBqlException {
        if (value instanceof ObjectValue)
            this.getObjectElements().addElement(name, (ObjectValue)value);
        else if (value instanceof TypedKeysAsColumnsValueMap)
            this.getKeysAsColumnsElements().addElement(name, (TypedKeysAsColumnsValueMap)value);
        else if (value instanceof FamilyDefaultValueMap)
            this.getFamilyDefaultElements().addElement(name, (FamilyDefaultValueMap)value);
        else if (value instanceof FamilyDefaultKeysAsColumnsValueMap)
            this.getFamilyDefaultKeysAsColumnsElements().addElement(name, (FamilyDefaultKeysAsColumnsValueMap)value);
        else
            throw new HBqlException("Internal error: " + value.getClass().getName());
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }

    public void clearValues() {
        this.getObjectElements().clear();
        this.getKeysAsColumnsElements().clear();
        this.getFamilyDefaultElements().clear();
        this.getFamilyDefaultKeysAsColumnsElements().clear();
    }

    // Simple get routines
    public ObjectValue getObjectValue(final String name, final boolean inSchema) throws HBqlException {
        final ObjectValue value = this.getObjectElements().findElement(name);
        if (value != null) {
            return value;
        }
        else {
            if (inSchema && !this.getSchema().containsVariableName(name))
                throw new HBqlException("Invalid variable name " + this.getSchema().getTableName() + "." + name);
            return new ObjectValue(this, name);
        }
    }

    public TypedKeysAsColumnsValueMap getKeysAsColumnsValue(final String name, final boolean inSchema) throws HBqlException {
        final TypedKeysAsColumnsValueMap value = this.getKeysAsColumnsElements().findElement(name);
        if (value != null) {
            return value;
        }
        else {
            if (inSchema && !this.getSchema().containsVariableName(name))
                throw new HBqlException("Invalid variable name " + this.getSchema().getTableName() + "." + name);
            return new TypedKeysAsColumnsValueMap(this, name);
        }
    }

    public FamilyDefaultValueMap getFamilyDefaultValue(final String name,
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

    public FamilyDefaultKeysAsColumnsValueMap getFamilyDefaultKeysAsColumnsValue(final String name) throws HBqlException {
        final FamilyDefaultKeysAsColumnsValueMap value = this.getFamilyDefaultKeysAsColumnsElements().findElement(name);
        return (value != null) ? value : new FamilyDefaultKeysAsColumnsValueMap(this, name);
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

    public boolean isCurrentValueSet(final ColumnAttrib attrib) throws HBqlException {
        final ObjectValue objectValue = this.getObjectElements().findElement(attrib.getAliasName());
        return objectValue != null && objectValue.isValueSet();
    }

    public Object getCurrentValue(final String name) throws HBqlException {
        final ObjectValue objectValue = this.getObjectElements().findElement(name);
        return (objectValue != null) ? objectValue.getValue() : null;
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
        final ObjectValue objectValue = this.getObjectValue(name, inSchema);
        objectValue.setCurrentValue(timestamp, val);
    }

    // Access to version maps
    public Map<Long, Object> getVersionMap(final String name) throws HBqlException {
        final ObjectValue value = this.getObjectElements().findElement(name);
        return (value != null) ? value.getVersionMap() : null;
    }

    public Map<Long, Object> getKeysAsColumnsVersionMap(final String columnName,
                                                        final String mapKey) throws HBqlException {
        final TypedKeysAsColumnsValueMap value = this.getKeysAsColumnsElements().findElement(columnName);
        return (value != null) ? value.getVersionMap(mapKey) : null;
    }

    public Map<String, byte[]> getFamilyDefaultValueMap(final String name) throws HBqlException {
        final FamilyDefaultValueMap value = this.getFamilyDefaultValue(name, false);
        if (value == null)
            return null;

        Map<String, byte[]> retval = Maps.newHashMap();
        for (final String key : value.getValueMap().keySet())
            retval.put(key, value.getValueMap().get(key).getValue());
        return retval;
    }

    public Map<Long, byte[]> getFamilyDefaultVersionMap(final String name,
                                                        final String columnName) throws HBqlException {
        final FamilyDefaultValueMap value = this.getFamilyDefaultValue(name, false);
        return (value != null) ? value.getVersionMap(columnName) : null;
    }

    public Map<String, NavigableMap<Long, byte[]>> getFamilyDefaultVersionMap(final String name) throws HBqlException {

        final FamilyDefaultValueMap value = this.getFamilyDefaultValue(name, false);
        if (value == null)
            return null;

        final Map<String, NavigableMap<Long, byte[]>> retval = Maps.newHashMap();
        for (final String key : value.getValueMap().keySet())
            retval.put(key, value.getValueMap().get(key).getVersionMap());
        return retval;
    }

    public Map<Long, UntypedKeysAsColumnsValueMap> getFamilyDefaultKeysAsColumnsVersionMap(final String name,
                                                                                           final String columnName) throws HBqlException {
        final FamilyDefaultKeysAsColumnsValueMap value = this.getFamilyDefaultKeysAsColumnsElements().findElement(name);
        return (value != null) ? value.getVersionMap(columnName) : null;
    }

    public void setVersionValue(final String name,
                                final long timestamp,
                                final Object val,
                                final boolean inSchema) throws HBqlException {
        final ObjectValue value = this.getObjectValue(name, inSchema);
        value.getVersionMap().put(timestamp, val);
    }

    public void setVersionValue(final String family,
                                final String column,
                                final long timestamp,
                                final Object val,
                                final boolean inSchema) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(family, column);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + family + ":" + column);
        this.setVersionValue(attrib.getColumnName(), timestamp, val, inSchema);
    }

    // KeysAsColumns values
    public Map<String, Object> getKeysAsColumnsMap(final String name) throws HBqlException {
        final TypedKeysAsColumnsValueMap map = this.getKeysAsColumnsElements().findElement(name);
        if (map == null)
            return null;

        final Map<String, Object> retval = Maps.newHashMap();
        for (final String key : map.getValueMap().keySet())
            retval.put(key, map.getValueMap().get(key));
        return retval;
    }

    public void setKeysAsColumnsValue(final String name,
                                      final String mapKey,
                                      final long timestamp,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {
        final TypedKeysAsColumnsValueMap value = this.getKeysAsColumnsValue(name, inSchema);
        value.setMapValue(timestamp, mapKey, val);
    }

    public void setKeysAsColumnsVersionMap(final String familyName,
                                           final String name,
                                           final NavigableMap<Long, byte[]> map) throws HBqlException {
        final TypedKeysAsColumnsValueMap value = this.getKeysAsColumnsElements().findElement(familyName);
        value.setVersionMap(name, (NavigableMap)map);
    }

    // FamilyDefault values
    public void setFamilyDefaultCurrentValue(final String familyName,
                                             final String name,
                                             final long timestamp,
                                             final byte[] val) throws HBqlException {
        final FamilyDefaultValueMap value = this.getFamilyDefaultValue(familyName + ":*", true);
        value.setMapValue(timestamp, name, val);
    }

    public void setFamilyDefaultVersionMap(final String familyName,
                                           final String name,
                                           final NavigableMap<Long, byte[]> val) throws HBqlException {
        final FamilyDefaultValueMap value = this.getFamilyDefaultValue(familyName + ":*", true);
        value.setVersionMap(name, val);
    }

    public void setFamilyDefaultKeysAsColumnsValue(final String familyName,
                                                   final String columnName,
                                                   final String mapKey,
                                                   final long timestamp,
                                                   final byte[] val) throws HBqlException {
        FamilyDefaultKeysAsColumnsValueMap value = this.getFamilyDefaultKeysAsColumnsElements().findElement(familyName);

        if (value == null) {
            value = new FamilyDefaultKeysAsColumnsValueMap(null, null);
            this.getFamilyDefaultKeysAsColumnsElements().addElement(familyName, value);
        }

        UntypedKeysAsColumnsValueMap kacMap = value.getCurrentMapValue(columnName);
        if (kacMap == null) {
            kacMap = new UntypedKeysAsColumnsValueMap(null, null);
            value.setMapValue(0, columnName, kacMap);
        }

        kacMap.setMapValue(timestamp, mapKey, val);
    }


    public void setFamilyDefaultKeysAsColumnsVersionMap(final String familyName,
                                                        final String columnName,
                                                        final String mapKey,
                                                        final NavigableMap<Long, byte[]> map) throws HBqlException {
        final FamilyDefaultKeysAsColumnsValueMap value =
                this.getFamilyDefaultKeysAsColumnsElements().findElement(familyName);
        final UntypedKeysAsColumnsValueMap kacValue = new UntypedKeysAsColumnsValueMap(null, null);
        kacValue.setVersionMap(columnName, map);
        value.setMapValue(0, mapKey, kacValue);
    }

    public Map<String, Map<String, byte[]>> getFamilyDefaultKeysAsColumnsMap(final String familyName) throws HBqlException {
        final FamilyDefaultKeysAsColumnsValueMap value =
                this.getFamilyDefaultKeysAsColumnsElements().findElement(familyName);

        Map<String, CurrentAndVersionValue<UntypedKeysAsColumnsValueMap>> map = value.getValueMap();

        final Map<String, Map<String, byte[]>> retval = Maps.newHashMap();
        for (final String columnName : map.keySet()) {
            final CurrentAndVersionValue<UntypedKeysAsColumnsValueMap> val = map.get(columnName);
            final Map<String, byte[]> newMap = Maps.newHashMap();
            retval.put(columnName, newMap);
            final Map<String, CurrentAndVersionValue<byte[]>> kacMap = val.getValue().getValueMap();
            for (final String mapKey : kacMap.keySet())
                newMap.put(mapKey, kacMap.get(mapKey).getValue());
        }
        return retval;
    }
}
