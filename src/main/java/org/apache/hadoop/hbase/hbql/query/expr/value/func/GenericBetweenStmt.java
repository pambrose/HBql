package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 2:00:25 PM
 */
public abstract class GenericBetweenStmt<T extends ValueExpr> extends GenericNotValue {

    private T expr = null;
    private T lower = null, upper = null;

    protected GenericBetweenStmt(final boolean not, final T expr, final T lower, final T upper) {
        super(not);
        this.expr = expr;
        this.lower = lower;
        this.upper = upper;
    }

    protected T getExpr() {
        return this.expr;
    }

    protected T getLower() {
        return this.lower;
    }

    protected T getUpper() {
        return this.upper;
    }

    public void setExpr(final T expr) {
        this.expr = expr;
    }

    public void setLower(final T lower) {
        this.lower = lower;
    }

    public void setUpper(final T upper) {
        this.upper = upper;
    }

    @Override
    public ValueExpr getOptimizedValue(final Object object) throws HPersistException {

        this.setExpr((T)this.getExpr().getOptimizedValue(object));
        this.setLower((T)this.getLower().getOptimizedValue(object));
        this.setUpper((T)this.getUpper().getOptimizedValue(object));

        return this.isAConstant() ? new BooleanLiteral(this.getValue(object)) : this;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        retval.addAll(this.getLower().getExprVariables());
        retval.addAll(this.getUpper().getExprVariables());
        return retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant() && this.getLower().isAConstant() && this.getUpper().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
        this.getLower().setContext(context);
        this.getUpper().setContext(context);
    }
}
