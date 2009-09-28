package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class NumberTernary extends GenericTernary implements NumberValue {

    public NumberTernary(final GenericValue arg0, final GenericValue arg1, final GenericValue arg2) {
        super(Type.NUMBERTERNARY, arg0, arg1, arg2);
    }

    @Override
    public Number getValue(final Object object) throws HBqlException {
        return (Number)super.getValue(object);
    }
}
