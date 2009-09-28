package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateBetweenStmt extends GenericBetweenStmt {

    public DateBetweenStmt(final GenericValue expr, final boolean not, final GenericValue lower, final GenericValue upper) {
        super(new TypeSignature(BooleanValue.class, DateValue.class, DateValue.class, DateValue.class),
              not,
              expr,
              lower,
              upper);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        final long dateval = (Long)this.getArg(0).getValue(object);
        final boolean retval = dateval >= (Long)this.getArg(1).getValue(object)
                               && dateval <= (Long)this.getArg(2).getValue(object);

        return (this.isNot()) ? !retval : retval;
    }
}