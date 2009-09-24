package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;

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

    protected void setPred(final BooleanValue pred) {
        this.pred = pred;
    }

    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getPred().getExprVariables();
        retval.addAll(this.getExpr1().getExprVariables());
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    public Object getValue(final Object object) throws HPersistException {
        if (((BooleanValue)this.getPred()).getValue(object))
            return this.getExpr1().getValue(object);
        else
            return this.getExpr2().getValue(object);
    }

    public boolean isAConstant() {
        return this.getPred().isAConstant() && this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

    public void setContext(final ExprTree context) {
        this.getPred().setContext(context);
        this.getExpr1().setContext(context);
        this.getExpr2().setContext(context);
    }

}
