package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class CompareExpr extends GenericTwoExprExpr<BooleanValue> implements BooleanValue {

    public enum OP {
        AND,
        OR
    }

    private final CompareExpr.OP op;

    public CompareExpr(final BooleanValue expr1, final CompareExpr.OP op, final BooleanValue expr2) {
        super(expr1, expr2);
        this.op = op;
    }

    private OP getOp() {
        return op;
    }

    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

        if (!type1.equals(type2))
            throw new HPersistException("Types in CompareExpr do not match");

        if (!ExprTree.isOfType(type1, BooleanValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in CompareExpr");

        return BooleanValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setExpr1((BooleanValue)this.getExpr1().getOptimizedValue());
        if (this.getExpr2() != null)
            this.setExpr2((BooleanValue)this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final boolean expr1val = this.getExpr1().getValue(object);

        if (this.getExpr2() == null)
            return expr1val;

        switch (this.getOp()) {
            case OR:
                return expr1val || this.getExpr2().getValue(object);
            case AND:
                return expr1val && this.getExpr2().getValue(object);

            default:
                throw new HPersistException("Error in BooleanExpr.getValue()");
        }
    }
}
