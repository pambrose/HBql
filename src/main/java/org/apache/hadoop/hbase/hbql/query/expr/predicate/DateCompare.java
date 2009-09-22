package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class DateCompare extends GenericCompare<DateValue> {

    public DateCompare(final DateValue expr1, final OP op, final DateValue expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr1().optimizeForConstants(object))
            this.setExpr1(new DateLiteral(this.getExpr1().getValue(object)));
        else
            retval = false;

        if (this.getExpr2().optimizeForConstants(object))
            this.setExpr2(new DateLiteral(this.getExpr2().getValue(object)));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final long val1 = this.getExpr1().getValue(object);
        final long val2 = this.getExpr2().getValue(object);

        switch (this.getOp()) {
            case EQ:
                return val1 == val2;
            case NOTEQ:
                return val1 != val2;
            case GT:
                return val1 > val2;
            case GTEQ:
                return val1 >= val2;
            case LT:
                return val1 < val2;
            case LTEQ:
                return val1 <= val2;
        }
        throw new HPersistException("Error in DateCompare.evaluate()");
    }

}