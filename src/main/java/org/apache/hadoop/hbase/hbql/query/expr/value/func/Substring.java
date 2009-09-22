package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class Substring implements StringValue {

    private StringValue expr = null;
    private NumberValue begin = null, end = null;

    public Substring(final StringValue expr, final NumberValue begin, final NumberValue end) {
        this.expr = expr;
        this.begin = begin;
        this.end = end;
    }

    private StringValue getExpr() {
        return this.expr;
    }

    private NumberValue getBegin() {
        return this.begin;
    }

    private NumberValue getEnd() {
        return this.end;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        retval.addAll(this.getBegin().getExprVariables());
        retval.addAll(this.getEnd().getExprVariables());
        return retval;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.expr = (StringValue)this.getExpr().getOptimizedValue();
        this.begin = (NumberValue)this.getBegin().getOptimizedValue();
        this.end = (NumberValue)this.getEnd().getOptimizedValue();

        return this.isAConstant() ? new StringLiteral(this.getValue(null)) : this;
    }

    @Override
    public String getValue(final Object object) throws HPersistException {
        final String val = this.getExpr().getValue(object);
        final int begin = this.getBegin().getValue(object).intValue();
        final int end = this.getEnd().getValue(object).intValue();
        return val.substring(begin, end);
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant() && this.getBegin().isAConstant() && this.getEnd().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
        this.getBegin().setContext(context);
        this.getEnd().setContext(context);
    }

}