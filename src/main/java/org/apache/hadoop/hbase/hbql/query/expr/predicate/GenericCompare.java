package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class GenericCompare extends GenericTwoExpr implements BooleanValue {

    private final Operator operator;

    protected GenericCompare(final GenericValue expr1, final Operator operator, final GenericValue expr2) {
        super(expr1, expr2);
        this.operator = operator;
    }

    protected Operator getOperator() {
        return this.operator;
    }

    @Override
    public GenericValue getOptimizedValue() throws HBqlException {
        this.setExpr1(this.getExpr1().getOptimizedValue());
        this.setExpr2(this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    protected Class<? extends GenericValue> validateType(final Class<? extends GenericValue> clazz) throws TypeException {
        HUtil.validateParentClass(this, clazz, this.getExpr1().validateTypes(this, false));
        HUtil.validateParentClass(this, clazz, this.getExpr2().validateTypes(this, false));

        return BooleanValue.class;
    }

    @Override
    public String asString() {
        final StringBuilder sbuf = new StringBuilder();
        sbuf.append(this.getExpr1().asString());
        sbuf.append(this.getOperator());
        sbuf.append(this.getExpr2().asString());

        return sbuf.toString();
    }

}