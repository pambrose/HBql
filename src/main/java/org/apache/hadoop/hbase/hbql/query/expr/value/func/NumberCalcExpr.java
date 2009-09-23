package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.NumberLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberCalcExpr extends GenericCalcExpr<NumberValue> implements NumberValue {

    public NumberCalcExpr(final NumberValue expr1, final GenericCalcExpr.OP op, final NumberValue expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();

        if (!ExprTree.isOfType(type1, NumberValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in NumberCalcExpr");

        if (this.getExpr2() != null) {
            final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

            if (!type1.equals(type2))
                throw new HPersistException("Type mismatch in NumberCalcExpr");
        }

        return NumberValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setExpr1((NumberValue)this.getExpr1().getOptimizedValue());
        if (this.getExpr2() != null)
            this.setExpr2((NumberValue)this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new NumberLiteral(this.getValue(null)) : this;
    }

    @Override
    public Long getValue(final Object object) throws HPersistException {

        final long val1 = this.getExpr1().getValue(object).longValue();
        final long val2 = (this.getExpr2() != null) ? (this.getExpr2().getValue(object)).longValue() : 0;

        switch (this.getOp()) {
            case PLUS:
                return val1 + val2;
            case MINUS:
                return val1 - val2;
            case MULT:
                return val1 * val2;
            case DIV:
                return val1 / val2;
            case MOD:
                return val1 % val2;
            case NEGATIVE:
                return val1 * -1;
        }

        throw new HPersistException("Error in NumberCalcExpr.getValue() " + this.getOp());

    }
}