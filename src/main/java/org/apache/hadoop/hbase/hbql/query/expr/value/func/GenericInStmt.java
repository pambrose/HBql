package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
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
public abstract class GenericInStmt<T extends ValueExpr> extends GenericNotValue {

    private T expr = null;
    private final List<T> valueList;

    protected GenericInStmt(final boolean not, final T expr, final List<T> valueList) {
        super(not);
        this.expr = expr;
        this.valueList = valueList;
    }

    protected T getExpr() {
        return expr;
    }

    protected void setExpr(final T expr) {
        this.expr = expr;
    }

    protected List<T> getValueList() {
        return valueList;
    }

    protected abstract boolean evaluateList(final Object object) throws HPersistException;

    protected abstract Class<? extends ValueExpr> getClassType() throws HPersistException;

    private void optimizeList() throws HPersistException {

        final List<T> newvalList = Lists.newArrayList();

        for (final T val : this.getValueList())
            newvalList.add((T)val.getOptimizedValue());

        // Swap new values to list
        this.getValueList().clear();
        this.getValueList().addAll(newvalList);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr().validateType();

        if (!ExprTree.isOfType(type1, this.getClassType()))
            throw new HPersistException("Type " + type1.getName() + " not valid in GenericInStmt");

        for (final T val : this.getValueList()) {
            final Class<? extends ValueExpr> type = val.validateType();

            if (!ExprTree.isOfType(type, this.getClassType()))
                throw new HPersistException("Type " + type1.getName() + " not valid in GenericInStmt");
        }
        return BooleanValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setExpr((T)this.getExpr().getOptimizedValue());

        this.optimizeList();

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        for (final T val : this.getValueList())
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
        for (final T value : this.getValueList())
            value.setContext(context);
    }

    private boolean listIsConstant() {

        for (final T val : this.getValueList()) {
            if (!val.isAConstant())
                return false;
        }
        return true;
    }
}