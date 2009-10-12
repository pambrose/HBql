package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.HRecordImpl;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class DefinedAttrib extends ColumnAttrib {

    private final ColumnDescription columnDescription;

    public DefinedAttrib(final ColumnDescription columnDescription) throws HBqlException {
        super(columnDescription.getFamilyName(),
              columnDescription.getColumnName(),
              columnDescription.getAliasName(),
              columnDescription.isMapKeysAsColumns(),
              columnDescription.getFieldType(),
              columnDescription.isArray(),
              null,
              null);

        this.columnDescription = columnDescription;

        if (this.isKeyAttrib() && this.getFamilyName().length() > 0)
            throw new HBqlException("Key value " + this.getNameToUseInExceptions() + " cannot have a family name");
    }

    private ColumnDescription getColumnDescription() {
        return this.columnDescription;
    }

    public String getColumnName() {
        return this.getColumnDescription().getColumnName();
    }

    public String getFamilyName() {
        return this.getColumnDescription().getFamilyName();
    }

    public String toString() {
        return this.getAliasName() + " - " + this.getFamilyQualifiedName();
    }

    public boolean isKeyAttrib() {
        return this.getFieldType() == FieldType.KeyType;
    }

    public Object getCurrentValue(final Object recordObj) throws HBqlException {
        return ((HRecordImpl)recordObj).getObjectCurrentValue(this.getAliasName());
    }

    public void setCurrentValue(final Object recordObj, final long timestamp, final Object val) throws HBqlException {
        ((HRecordImpl)recordObj).setObjectCurrentValue(this.getAliasName(), timestamp, val, true);
    }

    public void setKeysAsColumnsValue(final Object recordObj,
                                      final String mapKey,
                                      final Object val) throws HBqlException {

        if (!this.isMapKeysAsColumnsColumn())
            throw new HBqlException(this.getFamilyQualifiedName() + " not marked as mapKeysAsColumns");

        ((HRecord)recordObj).setKeysAsColumnsCurrentValue(this.getAliasName(), mapKey, 0, val, true);
    }

    public Map<Long, Object> getVersionObjectValueMap(final Object recordObj) throws HBqlException {
        return ((HRecordImpl)recordObj).getObjectHValue(this.getAliasName(), true).getVersionMap();
    }

    public Map<Long, Object> getKeysAsColumnsVersionMap(final Object recordObj,
                                                        final String mapKey) throws HBqlException {
        return ((HRecordImpl)recordObj).getKeysAsColumnsHValue(this.getAliasName(), true).getVersionMap(mapKey);
    }

    public Map<Long, byte[]> getFamilyDefaultVersionMap(final Object recordObj,
                                                        final String familyName) throws HBqlException {
        return ((HRecordImpl)recordObj).getFamilyDefaultHValue(this.getAliasName()).getVersionMap(familyName);
    }

    protected Method getMethod(final String methodName,
                               final Class<?>... params) throws NoSuchMethodException, HBqlException {
        throw new HBqlException("Internal error");
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
