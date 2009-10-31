package org.apache.hadoop.hbase.contrib.hbql.schema;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.contrib.hbql.impl.RecordImpl;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class DefinedAttrib extends ColumnAttrib {

    public DefinedAttrib(final ColumnDescription columnDescription) throws HBqlException {
        super(columnDescription.getFamilyName(),
              columnDescription.getColumnName(),
              columnDescription.getAliasName(),
              columnDescription.isMapKeysAsColumns(),
              columnDescription.isFamilyDefault(),
              columnDescription.getFieldType(),
              columnDescription.isArray(),
              null,
              null,
              columnDescription.getDefaultValue());

        if (this.isAKeyAttrib() && this.getFamilyName().length() > 0)
            throw new HBqlException("Key value " + this.getNameToUseInExceptions() + " cannot have a family name");
    }

    public String toString() {
        return this.getAliasName() + " - " + this.getFamilyQualifiedName();
    }

    public boolean isAKeyAttrib() {
        return this.getFieldType() == FieldType.KeyType;
    }

    protected void defineAccessors() {
        // No-op for Defined schema
    }

    public Object getCurrentValue(final Object hrecord) throws HBqlException {
        return ((RecordImpl)hrecord).getCurrentValue(this.getAliasName());
    }

    public void setCurrentValue(final Object hrecord, final long timestamp, final Object val) throws HBqlException {
        ((RecordImpl)hrecord).setCurrentValue(this.getAliasName(), timestamp, val, true);
    }

    public void setKeysAsColumnsValue(final Object hrecord,
                                      final String mapKey,
                                      final Object objval) throws HBqlException {

        if (!this.isMapKeysAsColumnsAttrib())
            throw new HBqlException(this.getFamilyQualifiedName() + " not marked as mapKeysAsColumns");

        ((RecordImpl)hrecord).setKeysAsColumnsValue(this.getAliasName(), mapKey, 0, objval, true);
    }

    public Map<Long, Object> getVersionMap(final Object hrecord) throws HBqlException {
        return ((RecordImpl)hrecord).getObjectValue(this.getAliasName(), true).getVersionMap(true);
    }

    public Map<Long, Object> getKeysAsColumnsVersionMap(final Object hrecord,
                                                        final String mapKey) throws HBqlException {
        return ((RecordImpl)hrecord).getKeysAsColumnsValueMap(this.getAliasName(), true).getVersionMap(mapKey, true);
    }

    public void setFamilyDefaultCurrentValue(final Object hrecord,
                                             final String name,
                                             final byte[] value) throws HBqlException {
        ((RecordImpl)hrecord).setFamilyDefaultCurrentValue(this.getFamilyName(), name, 0, value);
    }

    public void setFamilyDefaultVersionMap(final Object hrecord,
                                           final String name,
                                           final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {
        ((RecordImpl)hrecord).setFamilyDefaultVersionMap(this.getFamilyName(), name, timeStampMap);
    }

    public void setFamilyDefaultKeysAsColumnsValue(final Object hrecord,
                                                   final String columnName,
                                                   final String mapKey,
                                                   final byte[] valueBytes) throws HBqlException {
        ((RecordImpl)hrecord).setFamilyDefaultKeysAsColumnsValue(this.getFamilyName() + ":*",
                                                                 columnName,
                                                                 mapKey,
                                                                 0,
                                                                 valueBytes);
    }

    public void setFamilyDefaultKeysAsColumnsVersionMap(final Object obj,
                                                        final String columnName,
                                                        final String mapKey,
                                                        final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {

        ((RecordImpl)obj).setFamilyDefaultKeysAsColumnsVersionMap(this.getFamilyName(),
                                                                  columnName,
                                                                  mapKey,
                                                                  timeStampMap);
    }

    protected Method getMethod(final String methodName,
                               final Class<?>... params) throws NoSuchMethodException, HBqlException {
        throw new InternalErrorException();
    }

    protected Class getComponentType() {
        return this.getFieldType().getComponentType();
    }

    public String getNameToUseInExceptions() {
        return this.getFamilyQualifiedName();
    }

    public String getEnclosingClassName() {
        // TODO This will get resolved when getter/setter is added to DefinedSchema
        return "";
    }

    public boolean isAVersionValue() {
        return true;
    }

    public String[] getNamesForColumn() {
        final List<String> nameList = Lists.newArrayList();
        nameList.add(this.getFamilyQualifiedName());
        if (!this.getAliasName().equals(this.getFamilyQualifiedName()))
            nameList.add(this.getAliasName());
        return nameList.toArray(new String[nameList.size()]);
    }
}
