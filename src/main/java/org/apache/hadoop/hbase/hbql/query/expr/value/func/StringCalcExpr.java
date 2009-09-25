package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:51:01 PM
 */
public class StringCalcExpr extends GenericCalcExpr {

    public StringCalcExpr(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, op, expr2);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HBqlException {
        return this.validateType(StringValue.class, "StringCalcExpr");
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {

        this.setExpr1(this.getExpr1().getOptimizedValue());
        if (this.getExpr2() != null)
            this.setExpr2(this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new StringLiteral(this.getValue(null)) : this;
    }

    @Override
    public String getValue(final Object object) throws HBqlException {

        final String val1 = (String)this.getExpr1().getValue(object);
        final String val2 = (this.getExpr2() != null) ? ((String)this.getExpr2().getValue(object)) : "";

        switch (this.getOp()) {
            case PLUS:
                return val1 + val2;
        }

        throw new HBqlException("Invalid operator in StringCalcExpr.getValue() " + this.getOp());
    }

}