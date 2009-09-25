package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class DateCompare extends GenericCompare {

    public DateCompare(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

        if (!HUtil.isParentClass(DateValue.class, type1, type2))
            throw new HPersistException("Invalid types "
                                        + type1.getName() + " " + type2.getName() + " in DateCompare");

        return BooleanValue.class;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final long val1 = (Long)this.getExpr1().getValue(object);
        final long val2 = (Long)this.getExpr2().getValue(object);

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
        throw new HPersistException("Error in DateCompare.getValue()");
    }

}