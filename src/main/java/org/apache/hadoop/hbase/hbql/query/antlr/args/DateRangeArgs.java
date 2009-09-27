package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 9:53:48 AM
 */
public class DateRangeArgs {

    private final GenericValue lower;
    private final GenericValue upper;

    public DateRangeArgs(final GenericValue lower, final GenericValue upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public boolean isValid() {
        return this.lower != null && this.upper != null;
    }

    public long getLower() throws HBqlException {

        if (this.lower == null)
            throw new HBqlException("Null value not valid");

        final Class clazz = this.lower.getClass();
        if (!HUtil.isParentClass(DateValue.class, clazz))
            throw new HBqlException("Invalid type " + clazz.getSimpleName());

        return (Long)this.lower.getValue(null);
    }

    public long getUpper() throws HBqlException {

        if (this.upper == null)
            throw new HBqlException("Null value not valid");

        final Class clazz = this.upper.getClass();
        if (!HUtil.isParentClass(DateValue.class, clazz))
            throw new HBqlException("Invalid type " + clazz.getSimpleName());

        return (Long)this.upper.getValue(null);
    }
}