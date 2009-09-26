package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberBetweenStmt extends GenericBetweenStmt {

    public NumberBetweenStmt(final ValueExpr expr, final boolean not, final ValueExpr lower, final ValueExpr upper) {
        super(not, expr, lower, upper);
    }

    @Override
    public Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr) throws TypeException {
        return this.validateType(NumberValue.class);
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        final long numval = ((Number)this.getExpr().getValue(object)).longValue();
        final boolean retval = numval >= ((Number)this.getLower().getValue(object)).longValue()
                               && numval <= ((Number)this.getUpper().getValue(object)).longValue();

        return (this.isNot()) ? !retval : retval;
    }

}