package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.NumberLiteral;

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
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.setExpr(new NumberLiteral(this.getExpr().getValue(object)));
        else
            retval = false;

        if (this.getLower().optimizeForConstants(object))
            this.setLower(new NumberLiteral(this.getLower().getValue(object)));
        else
            retval = false;

        if (this.getUpper().optimizeForConstants(object))
            this.setUpper(new NumberLiteral(this.getUpper().getValue(object)));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {

        final long numval = this.getExpr().getValue(object).longValue();
        final boolean retval = numval >= this.getLower().getValue(object).longValue()
                               && numval <= this.getUpper().getValue(object).longValue();

        return (this.isNot()) ? !retval : retval;
    }

}