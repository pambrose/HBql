package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:26:18 AM
 */
public class LimitArgs {

    private final ValueExpr value;

    public LimitArgs(final ValueExpr value) {
        this.value = value;
    }

    public boolean isValid() {
        return this.value != null;
    }

    public long getValue() throws HBqlException {

        if (this.value == null)
            throw new HBqlException("Null value invalid in LimitArgs");

        final Class clazz = this.value.getClass();
        if (!NumberValue.class.isAssignableFrom(clazz))
            throw new HBqlException("Invalid type " + clazz.getSimpleName() + " in LimitArgs");

        return ((Number)this.value.getValue(null)).longValue();
    }
}