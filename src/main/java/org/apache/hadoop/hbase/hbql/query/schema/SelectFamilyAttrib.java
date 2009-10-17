package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InternalErrorException;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;

public class SelectFamilyAttrib extends ColumnAttrib {

    public SelectFamilyAttrib(final String familyName) {
        super(familyName, "", "", false, false, null, false, null, null);
    }

    public boolean isASelectFamilyAttrib() {
        return true;
    }

    public String getNameToUseInExceptions() {
        return this.getFamilyQualifiedName();
    }

    public Map<Long, Object> getVersionMap(final Object recordObj) throws HBqlException {
        throw new InternalErrorException();
    }

    protected Class getComponentType() throws HBqlException {
        throw new InternalErrorException();
    }

    public Map<Long, Object> getKeysAsColumnsVersionMap(final Object obj, final String mapKey) throws HBqlException {
        throw new InternalErrorException();
    }

    public Object getCurrentValue(final Object obj) throws HBqlException {
        throw new InternalErrorException();
    }

    protected Method getMethod(final String methodName, final Class<?>... params) throws NoSuchMethodException {
        return null;
    }

    public String getEnclosingClassName() {
        return null;
    }

    public void setCurrentValue(final Object obj, final long timestamp, final Object val) throws HBqlException {
        throw new InternalErrorException();
    }

    public void setKeysAsColumnsValue(final Object obj,
                                      final String mapKey,
                                      final Object val) throws HBqlException {
        throw new InternalErrorException();
    }

    public void setFamilyDefaultCurrentValue(final Object obj,
                                             final String name,
                                             final byte[] value) throws HBqlException {
        throw new InternalErrorException();
    }

    public void setFamilyDefaultVersionMap(final Object obj,
                                           final String name,
                                           final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {
        throw new InternalErrorException();
    }

    public void setFamilyDefaultKeysAsColumnsValue(final Object obj, final String columnName,
                                                   final String mapKey,
                                                   final byte[] valueBytes) throws HBqlException {
        throw new InternalErrorException();
    }

    public void setFamilyDefaultKeysAsColumnsVersionMap(final Object obj,
                                                        final String columnName,
                                                        final String mapKey, final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {
        throw new InternalErrorException();
    }
}
