package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 10, 2009
 * Time: 11:33:09 AM
 */
public class GenericOneExprExpr {

    private ValueExpr expr = null;

    public GenericOneExprExpr(final ValueExpr expr) {
        this.expr = expr;
    }

    protected ValueExpr getExpr() {
        return this.expr;
    }

    protected void setExpr(final ValueExpr expr) {
        this.expr = expr;
    }

    public List<ExprVariable> getExprVariables() {
        return this.getExpr().getExprVariables();
    }

    public boolean isAConstant() {
        return this.getExpr().isAConstant();
    }

    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
    }

    public void setParam(final String param, final Object val) throws HPersistException {
        this.getExpr().setParam(param, val);
    }
}