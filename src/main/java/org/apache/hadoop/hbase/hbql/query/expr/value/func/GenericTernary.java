package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.util.List;

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
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getPred().getExprVariables();
        retval.addAll(this.getExpr1().getExprVariables());
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public Object getValue(final Object object) throws HPersistException {
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

    @Override
    public void setParam(final String param, final Object val) throws HPersistException {
        this.getPred().setParam(param, val);
        this.getExpr1().setParam(param, val);
        this.getExpr2().setParam(param, val);
    }

    protected Class<? extends ValueExpr> validateType(final Class<? extends ValueExpr> clazz, final String caller) throws HPersistException {

        final Class<? extends ValueExpr> pred = this.getPred().validateType();
        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

        if (HUtil.isParentClass(BooleanValue.class, pred))
            throw new HPersistException("Invalid predicate type " + pred.getName() + " in NumberTernary");

        if (HUtil.isParentClass(clazz, type1, type2))
            throw new HPersistException("Invalid types " + type1.getName() + " " + type2.getName() + " in " + caller);

        return clazz;
    }

}
