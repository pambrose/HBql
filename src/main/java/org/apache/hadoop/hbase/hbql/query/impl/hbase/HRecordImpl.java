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

    private ElementMap<ObjectValue> objectElements = null;
    private ElementMap<TypedKeysAsColumnsValueMap> keysAsColumnsElements = null;
    private ElementMap<FamilyDefaultValueMap> familyDefaultElements = null;
    private ElementMap<FamilyDefaultKeysAsColumnsValueMap> familyDefaultKeysAsColumnsElements = null;

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
        if (this.objectElements != null)
            return this.objectElements;

        synchronized (this) {
            if (this.objectElements == null)
                this.objectElements = new ElementMap<ObjectValue>(this);
            return this.objectElements;
        }
    }

    public ElementMap<TypedKeysAsColumnsValueMap> getKeysAsColumnsElements() {
        if (this.keysAsColumnsElements != null)
            return this.keysAsColumnsElements;

        synchronized (this) {
            if (this.keysAsColumnsElements == null)
                this.keysAsColumnsElements = new ElementMap<TypedKeysAsColumnsValueMap>(this);
            return this.keysAsColumnsElements;
        }
    }

    public ElementMap<FamilyDefaultValueMap> getFamilyDefaultElements() {
        if (this.familyDefaultElements != null)
            return this.familyDefaultElements;

        synchronized (this) {
            if (this.familyDefaultElements == null)
                this.familyDefaultElements = new ElementMap<FamilyDefaultValueMap>(this);
            return this.familyDefaultElements;
        }
    }

    public ElementMap<FamilyDefaultKeysAsColumnsValueMap> getFamilyDefaultKeysAsColumnsElements() {
        if (this.familyDefaultKeysAsColumnsElements != null)
            return this.familyDefaultKeysAsColumnsElements;

        synchronized (this) {
            if (this.familyDefaultKeysAsColumnsElements == null)
                this.familyDefaultKeysAsColumnsElements = new ElementMap<FamilyDefaultKeysAsColumnsValueMap>(this);
            return this.familyDefaultKeysAsColumnsElements;
        }
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

    public TypedKeysAsColumnsValueMap getKeysAsColumnsValueMap(final String name, final boolean inSchema) throws HBqlException {
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

    public FamilyDefaultValueMap getFamilyDefaultValueMap(final String name,
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

    public FamilyDefaultKeysAsColumnsValueMap getFamilyDefaultKeysAsColumnsValueMap(final String name,
                                                                                    final boolean createNewIfMissing) throws HBqlException {
        final FamilyDefaultKeysAsColumnsValueMap value = this.getFamilyDefaultKeysAsColumnsElements().findElement(name);
        if (value != null) {
            return value;
        }
        else {
            if (createNewIfMissing)
                return new FamilyDefaultKeysAsColumnsValueMap(this, name);
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

    public boolean isCurrentValueSet(final ColumnAttrib attrib) throws HBqlException {
        final ObjectValue objectValue = this.getObjectElements().findElement(attrib.getAliasName());
        return objectValue != null && objectValue.isValueSet();
    }


    public void setCurrentValue(final String name,
                                final long timestamp,
                                final Object val,
                                final boolean inSchema) throws HBqlException {
        this.getObjectValue(name, inSchema).setCurrentValue(timestamp, val);
    }

    public Map<Long, byte[]> getFamilyDefaultVersionMap(final String name,
                                                        final String columnName) throws HBqlException {
        final FamilyDefaultValueMap value = this.getFamilyDefaultValueMap(name, false);
        return (value != null) ? value.getVersionMap(columnName, true) : null;
    }

    public Map<Long, UntypedKeysAsColumnsValueMap> getFamilyDefaultKeysAsColumnsVersionMap(final String familyName,
                                                                                           final String columnName) throws HBqlException {
        final FamilyDefaultKeysAsColumnsValueMap value = this.getFamilyDefaultKeysAsColumnsValueMap(familyName, false);
        return (value != null) ? value.getVersionMap(columnName, true) : null;
    }

    public void setVersionValue(final String familyName,
                                final String columnName,
                                final long timestamp,
                                final Object val,
                                final boolean inSchema) throws HBqlException {
        final ColumnAttrib attrib = this.getSchema().getAttribFromFamilyQualifiedName(familyName, columnName);
        if (attrib == null)
            throw new HBqlException("Invalid column name " + familyName + ":" + columnName);

        this.getObjectValue(attrib.getColumnName(), inSchema).getVersionMap(true).put(timestamp, val);
    }

    public void setKeysAsColumnsValue(final String name,
                                      final String mapKey,
                                      final long timestamp,
                                      final Object val,
                                      final boolean inSchema) throws HBqlException {
        final TypedKeysAsColumnsValueMap value = this.getKeysAsColumnsValueMap(name, inSchema);
        value.setCurrentValueMap(timestamp, mapKey, val);
    }

    public void setKeysAsColumnsVersionMap(final String familyName,
                                           final String name,
                                           final NavigableMap<Long, byte[]> map) throws HBqlException {
        final TypedKeysAsColumnsValueMap value = this.getKeysAsColumnsElements().findElement(familyName);
        value.setVersionMap(name, (NavigableMap)map);
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

    public void setFamilyDefaultKeysAsColumnsValue(final String familyName,
                                                   final String columnName,
                                                   final String mapKey,
                                                   final long timestamp,
                                                   final byte[] val) throws HBqlException {
        final FamilyDefaultKeysAsColumnsValueMap value = this.getFamilyDefaultKeysAsColumnsValueMap(familyName, true);
        value.getCurrentMapValue(columnName, true).setCurrentValueMap(timestamp, mapKey, val);
    }

    public void setFamilyDefaultKeysAsColumnsVersionMap(final String familyName,
                                                        final String columnName,
                                                        final String mapKey,
                                                        final NavigableMap<Long, byte[]> map) throws HBqlException {
        final FamilyDefaultKeysAsColumnsValueMap value = this.getFamilyDefaultKeysAsColumnsValueMap(familyName, true);
        value.getCurrentMapValue(columnName, true).setVersionMap(mapKey, map);
    }

    public Object getValue(final String name) throws HBqlException {
        final ObjectValue objectValue = this.getObjectElements().findElement(name);
        return (objectValue != null) ? objectValue.getValue() : null;
    }

    public void setValue(final String name, final Object val) throws HBqlException {
        this.setCurrentValue(name, this.getTimestamp(), val, true);
    }

    public Map<Long, Object> getVersionMap(final String name) throws HBqlException {
        final ObjectValue value = this.getObjectElements().findElement(name);
        return (value != null) ? value.getVersionMap(true) : null;
    }

    public Map<String, NavigableMap<Long, Object>> getKeysAsColumnsVersionMap(final String columnName) throws HBqlException {

        final TypedKeysAsColumnsValueMap value = this.getKeysAsColumnsElements().findElement(columnName);

        if (value == null)
            return null;

        final Map<String, NavigableMap<Long, Object>> retval = Maps.newHashMap();
        for (final String key : value.getCurrentAndVersionMap().keySet())
            retval.put(key, value.getCurrentAndVersionMap().get(key).getVersionMap(true));
        return retval;
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

    public Map<String, Object> getKeysAsColumnsMap(final String name) throws HBqlException {

        final TypedKeysAsColumnsValueMap value = this.getKeysAsColumnsElements().findElement(name);
        if (value == null)
            return null;

        final Map<String, Object> retval = Maps.newHashMap();
        for (final String key : value.getCurrentAndVersionMap().keySet())
            retval.put(key, value.getCurrentAndVersionMap().get(key));
        return retval;
    }

    public Map<String, Map<String, byte[]>> getFamilyDefaultKeysAsColumnsMap(final String familyName) throws HBqlException {

        final FamilyDefaultKeysAsColumnsValueMap value =
                this.getFamilyDefaultKeysAsColumnsValueMap(familyName, false);
        if (value == null)
            return null;

        final Map<String, Map<String, byte[]>> retval = Maps.newHashMap();
        final Map<String, CurrentAndVersionValue<UntypedKeysAsColumnsValueMap>> map = value.getCurrentAndVersionMap();
        for (final String columnName : map.keySet()) {

            final Map<String, byte[]> newMap = Maps.newHashMap();
            retval.put(columnName, newMap);

            final CurrentAndVersionValue<UntypedKeysAsColumnsValueMap> val = map.get(columnName);
            final Map<String, CurrentAndVersionValue<byte[]>> kacMap = val.getValue().getCurrentAndVersionMap();
            for (final String mapKey : kacMap.keySet())
                newMap.put(mapKey, kacMap.get(mapKey).getValue());
        }
        return retval;
    }

    public Map<String, Map<String, NavigableMap<Long, byte[]>>> getFamilyDefaultKeysAsColumnsVersionMap(final String familyName) throws HBqlException {

        final FamilyDefaultKeysAsColumnsValueMap value =
                this.getFamilyDefaultKeysAsColumnsValueMap(familyName, false);

        if (value == null)
            return null;

        final Map<String, Map<String, NavigableMap<Long, byte[]>>> retval = Maps.newHashMap();
        final Map<String, CurrentAndVersionValue<UntypedKeysAsColumnsValueMap>> map = value.getCurrentAndVersionMap();
        for (final String columnName : map.keySet()) {

            final Map<String, NavigableMap<Long, byte[]>> newMap = Maps.newHashMap();
            retval.put(columnName, newMap);

            final CurrentAndVersionValue<UntypedKeysAsColumnsValueMap> val = map.get(columnName);
            final Map<String, CurrentAndVersionValue<byte[]>> kacMap = val.getValue().getCurrentAndVersionMap();
            for (final String mapKey : kacMap.keySet())
                newMap.put(mapKey, kacMap.get(mapKey).getVersionMap(true));
        }
        return retval;
    }
}
