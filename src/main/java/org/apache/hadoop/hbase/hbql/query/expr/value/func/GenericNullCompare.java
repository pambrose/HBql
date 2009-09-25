package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public abstract class GenericNullCompare extends GenericNotValue {

    private ValueExpr expr = null;

    public GenericNullCompare(final boolean not, final ValueExpr expr) {
        super(not);
        this.expr = expr;
    }

    protected ValueExpr getExpr() {
        return this.expr;
    }

    protected void setExpr(final ValueExpr expr) {
        this.expr = expr;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return this.getExpr().getExprVariables();
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
    }

    @Override
    public void setParam(final String param, final Object val) throws HPersistException {
        this.getExpr().setParam(param, val);
    }

    protected Class<? extends ValueExpr> validateType(final Class<? extends ValueExpr> clazz, final String caller) throws HPersistException {
        final Class<? extends ValueExpr> type = this.getExpr().validateType();
        if (!HUtil.isParentClass(clazz, type))
            throw new HPersistException("Invalid type " + type.getName() + " in " + caller);
        return BooleanValue.class;
    }
}