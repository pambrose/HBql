package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 1, 2009
 * Time: 8:07:49 PM
 */
public class FamilyAttrib extends ColumnAttrib {

    public FamilyAttrib(final String familyName) {
        super(familyName, "", "", null, true, null, null);
    }

    public boolean isAFamilyAttrib() {
        return true;
    }

    @Override
    public boolean isArray() {
        return false;
    }

    @Override
    protected Method getMethod(final String methodName, final Class<?>... params) throws NoSuchMethodException {
        return null;
    }

    @Override
    public String getEnclosingClassName() {
        return null;
    }

    @Override
    public Object getCurrentValue(final Object recordObj) throws HBqlException {
        return null;
    }

    @Override
    public String getObjectQualifiedName() {
        return null;
    }

    @Override
    public void setCurrentValue(final Object newobj, final long timestamp, final Object val) throws HBqlException {

    }

    @Override
    public Map<Long, Object> getVersionValueMapValue(final Object recordObj) throws HBqlException {
        return null;
    }

    @Override
    protected Class getComponentType() {
        return null;
    }

    @Override
    public void setVersionValueMapValue(final Object newobj, final Map<Long, Object> map) {

    }
}
