package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

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
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr().validateType();
        final Class<? extends ValueExpr> type2 = this.getLower().validateType();
        final Class<? extends ValueExpr> type3 = this.getUpper().validateType();

        if (!ExprTree.isOfType(type1, NumberValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in NumberBetweenStmt");

        if (!ExprTree.isOfType(type2, NumberValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in NumberBetweenStmt");

        if (!ExprTree.isOfType(type3, NumberValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in NumberBetweenStmt");

        return BooleanValue.class;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final long numval = this.getExpr().getValue(object).longValue();
        final boolean retval = numval >= this.getLower().getValue(object).longValue()
                               && numval <= this.getUpper().getValue(object).longValue();

        return (this.isNot()) ? !retval : retval;
    }

}