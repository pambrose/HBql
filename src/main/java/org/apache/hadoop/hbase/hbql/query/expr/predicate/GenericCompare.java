package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.func.Operator;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public abstract class GenericCompare extends GenericTwoExprExpr implements BooleanValue {

    private final Operator op;

    protected GenericCompare(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(expr1, expr2);
        this.op = op;
    }

    protected Operator getOp() {
        return op;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HBqlException {
        this.setExpr1(this.getExpr1().getOptimizedValue());
        if (this.getExpr2() != null)
            this.setExpr2(this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    protected Class<? extends ValueExpr> validateType(final Class<? extends ValueExpr> clazz, final String caller) throws HBqlException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();

        if (!HUtil.isParentClass(clazz, type1))
            throw new HBqlException("Invalid type "
                                    + type1.getName() + " in " + caller);

        if (this.getExpr2() != null) {
            final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();
            if (!HUtil.isParentClass(clazz, type2))
                throw new HBqlException("Invalid types " + type2.getName() + " in " + caller);
        }

        return BooleanValue.class;
    }

}