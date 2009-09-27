package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 7, 2009
 * Time: 9:29:44 PM
 */
public abstract class GenericCalculation extends GenericTwoExpr implements GenericValue {

    private final Operator operator;

    protected GenericCalculation(final GenericValue expr1, final Operator operator, final GenericValue expr2) {
        super(expr1, expr2);
        this.operator = operator;
    }

    protected Operator getOperator() {
        return this.operator;
    }

    protected Class<? extends GenericValue> validateType(final Class<? extends GenericValue> clazz) throws TypeException {
        HUtil.validateParentClass(this, clazz, this.getExpr1().validateTypes(this, false));
        HUtil.validateParentClass(this, clazz, this.getExpr2().validateTypes(this, false));

        return clazz;
    }

    @Override
    public String asString() {
        if (this.getOperator() == Operator.NEGATIVE)
            return "-" + this.getExpr1().asString();
        else
            return this.getExpr1().asString() + " " + this.getOperator() + " " + this.getExpr2().asString();
    }
}
