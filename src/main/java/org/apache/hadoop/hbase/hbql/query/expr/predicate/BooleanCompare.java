package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class BooleanCompare extends GenericCompare implements BooleanValue {

    public BooleanCompare(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    public Class<? extends ValueExpr> validateType() throws HPersistException {
        return this.validateType(BooleanValue.class, "BooleanCompare");
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final boolean expr1val = (Boolean)this.getExpr1().getValue(object);

        if (this.getExpr2() == null)
            throw new HPersistException("Null value in BooleanCompare.getValue()");

        final boolean expr2val = (Boolean)this.getExpr2().getValue(object);

        switch (this.getOp()) {
            case OR:
                return expr1val || expr2val;
            case AND:
                return expr1val && expr2val;
            case EQ:
                return expr1val == expr2val;
            case NOTEQ:
                return expr1val != expr2val;
            default:
                throw new HPersistException("Error in BooleanCompare.getValue()");
        }
    }
}
