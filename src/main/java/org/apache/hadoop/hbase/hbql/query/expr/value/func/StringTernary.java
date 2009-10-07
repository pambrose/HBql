package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class StringTernary extends GenericTernary {

    public StringTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(Type.STRINGTERNARY, arg0, arg1, arg2);
    }

    public String getValue(final Object object) throws HBqlException {
        return (String)super.getValue(object);
    }
}