package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 9:53:48 AM
 */
public class DateRangeArgs {

    private final ValueExpr lower;
    private final ValueExpr upper;

    public DateRangeArgs(final ValueExpr lower, final ValueExpr upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public boolean isValid() {
        return this.lower != null && this.upper != null;
    }

    public long getLower() throws HPersistException {

        if (this.lower == null)
            throw new HPersistException("Null value invalid in DateRangeArgs");

        final Class clazz = this.lower.getClass();
        if (!clazz.equals(DateValue.class))
            throw new HPersistException("Invalid type " + clazz.getName() + " in DateRangeArgs");

        return (Long)this.lower.getValue(null);
    }

    public long getUpper() throws HPersistException {

        if (this.upper == null)
            throw new HPersistException("Null value invalid in DateRangeArgs");

        final Class clazz = this.upper.getClass();
        if (!clazz.equals(DateValue.class))
            throw new HPersistException("Invalid type " + clazz.getName() + " in DateRangeArgs");

        return (Long)this.upper.getValue(null);
    }
}