package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.lang.reflect.Method;
import java.util.Map;

public class FamilyAttrib extends ColumnAttrib {

    public FamilyAttrib(final String familyName) {
        super(familyName, "", "", false, false, null, false, null, null);
    }

    public boolean isAFamilyAttrib() {
        return true;
    }

    public String getNameToUseInExceptions() {
        return this.getFamilyQualifiedName();
    }

    public Map<Long, Object> getVersionMap(final Object recordObj) throws HBqlException {
        throw new HBqlException("Internal error");
    }

    protected Class getComponentType() throws HBqlException {
        throw new HBqlException("Internal error");
    }

    public Map<Long, Object> getKeysAsColumnsVersionMap(final Object newobj, final String mapKey) throws HBqlException {
        throw new HBqlException("Internal error");
    }

    public Object getCurrentValue(final Object recordObj) throws HBqlException {
        throw new HBqlException("Internal error");
    }

    protected Method getMethod(final String methodName, final Class<?>... params) throws NoSuchMethodException {
        return null;
    }

    public String getEnclosingClassName() {
        return null;
    }

    public void setCurrentValue(final Object newobj, final long timestamp, final Object val) throws HBqlException {
        throw new HBqlException("Internal error");
    }

    public void setKeysAsColumnsValue(final Object newobj,
                                      final String mapKey,
                                      final Object val) throws HBqlException {
        throw new HBqlException("Internal error");
    }

    public void setFamilyDefaultCurrentValue(final Object obj,
                                             final String name,
                                             final long timestamp,
                                             final byte[] value) throws HBqlException {
        throw new HBqlException("Internal error");
    }
}
