package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class GenericNullCompare<T extends ValueExpr> extends GenericNotValue {

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

    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type = this.getExpr().validateType();

        if (!ExprTree.isOfType(type, StringValue.class))
            throw new HPersistException("Type " + type.getName() + " not valid in StringNullCompare");

        return BooleanValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        this.setExpr((StringValue)this.getExpr().getOptimizedValue());
        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        final String val = this.getExpr().getValue(object);
        final boolean retval = (val == null);
        return (this.isNot()) ? !retval : retval;
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