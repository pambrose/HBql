package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericInStmt extends GenericNotValue {

    private ValueExpr expr = null;
    private final List<ValueExpr> valueList;

    protected GenericInStmt(final boolean not, final ValueExpr expr, final List<ValueExpr> valueList) {
        super(not);
        this.expr = expr;
        this.valueList = valueList;
    }

    protected ValueExpr getExpr() {
        return expr;
    }

    protected void setExpr(final ValueExpr expr) {
        this.expr = expr;
    }

    protected List<ValueExpr> getValueList() {
        return valueList;
    }

    protected abstract boolean evaluateList(final Object object) throws HPersistException;

    private void optimizeList() throws HPersistException {

        final List<ValueExpr> newvalList = Lists.newArrayList();

        for (final ValueExpr val : this.getValueList())
            newvalList.add(val.getOptimizedValue());

        // Swap new values to list
        this.getValueList().clear();
        this.getValueList().addAll(newvalList);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {
        return null;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        this.setExpr(this.getExpr().getOptimizedValue());
        this.optimizeList();
        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        for (final ValueExpr val : this.getValueList())
            retval.addAll(val.getExprVariables());
        return retval;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        final boolean retval = this.evaluateList(object);
        return (this.isNot()) ? !retval : retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant() && this.listIsConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
        for (final ValueExpr value : this.getValueList())
            value.setContext(context);
    }

    private boolean listIsConstant() {

        for (final ValueExpr val : this.getValueList()) {
            if (!val.isAConstant())
                return false;
        }
        return true;
    }
}