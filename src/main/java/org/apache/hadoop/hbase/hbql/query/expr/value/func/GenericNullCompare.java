package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public abstract class GenericNullCompare<T extends ValueExpr> extends GenericNotValue {

    private T expr = null;

    public GenericNullCompare(final boolean not, final T expr) {
        super(not);
        this.expr = expr;
    }

    protected T getExpr() {
        return this.expr;
    }

    protected void setExpr(final T expr) {
        this.expr = expr;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return this.getExpr().getExprVariables();
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {
        return null;
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
    }

}