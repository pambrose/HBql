package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class DateTernary extends GenericTernary<DateValue> implements DateValue {


    public DateTernary(final BooleanValue pred, final DateValue expr1, final DateValue expr2) {
        super(pred, expr1, expr2);
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getPred().optimizeForConstants(object))
            this.setPred(new BooleanLiteral(this.getPred().getValue(object)));
        else
            retval = false;

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
    public Long getValue(final Object object) throws HPersistException {
        return (Long)super.getValue(object);
    }
}