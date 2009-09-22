package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberBetweenStmt extends GenericBetweenStmt<NumberValue> {

    public NumberBetweenStmt(final NumberValue expr, final boolean not, final NumberValue lower, final NumberValue upper) {
        super(not, expr, lower, upper);
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final long numval = this.getExpr().getValue(object).longValue();
        final boolean retval = numval >= this.getLower().getValue(object).longValue()
                               && numval <= this.getUpper().getValue(object).longValue();

        return (this.isNot()) ? !retval : retval;
    }

}