package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberBetweenStmt extends GenericBetweenStmt<ValueExpr> {

    public NumberBetweenStmt(final ValueExpr expr, final boolean not, final ValueExpr lower, final ValueExpr upper) {
        super(not, expr, lower, upper);
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final long numval = ((NumberValue)this.getExpr()).getValue(object).longValue();
        final boolean retval = numval >= ((NumberValue)this.getLower()).getValue(object).longValue()
                               && numval <= ((NumberValue)this.getUpper()).getValue(object).longValue();

        return (this.isNot()) ? !retval : retval;
    }

}