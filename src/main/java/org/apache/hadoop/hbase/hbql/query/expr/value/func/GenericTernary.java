package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 1:51:03 PM
 */
public abstract class GenericTernary extends GenericTwoExprExpr implements ValueExpr {

    private ValueExpr pred = null;

    protected GenericTernary(final ValueExpr pred, final ValueExpr expr1, final ValueExpr expr2) {
        super(expr1, expr2);
        this.pred = pred;
    }

    protected ValueExpr getPred() {
        return this.pred;
    }

    protected void setPred(final ValueExpr pred) {
        this.pred = pred;
    }

    @Override
    public Object getValue(final Object object) throws HBqlException {
        if ((Boolean)this.getPred().getValue(object))
            return this.getExpr1().getValue(object);
        else
            return this.getExpr2().getValue(object);
    }

    @Override
    public boolean isAConstant() {
        return this.getPred().isAConstant() && this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getPred().setContext(context);
        this.getExpr1().setContext(context);
        this.getExpr2().setContext(context);
    }

    protected Class<? extends ValueExpr> validateType(final Class<? extends ValueExpr> clazz) throws TypeException {
        HUtil.validateParentClass(this, BooleanValue.class, this.getPred().validateTypes());
        HUtil.validateParentClass(this,
                                  clazz,
                                  this.getExpr1().validateTypes(),
                                  this.getExpr2().validateTypes());
        return clazz;
    }

    @Override
    public String asString() {
        return "IF " + this.getPred().asString() + " THEN "
               + this.getExpr1().asString() + " ELSE " + this.getExpr2().asString() + " END";
    }

}
