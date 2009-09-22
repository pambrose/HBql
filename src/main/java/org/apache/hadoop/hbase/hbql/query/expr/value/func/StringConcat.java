package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 6:20:20 PM
 */
public class StringConcat implements StringValue {

    private StringValue expr1 = null, expr2 = null;

    public StringConcat(final StringValue expr1, StringValue expr2) {
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    private StringValue getExpr1() {
        return this.expr1;
    }

    private void setExpr1(final StringValue expr1) {
        this.expr1 = expr1;
    }

    private StringValue getExpr2() {
        return this.expr2;
    }

    private void setExpr2(final StringValue expr2) {
        this.expr2 = expr2;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = getExpr1().getExprVariables();
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public ValueExpr getOptimizedValue(final Object object) throws HPersistException {

        this.setExpr1((StringValue)this.getExpr1().getOptimizedValue(object));
        this.setExpr2((StringValue)this.getExpr2().getOptimizedValue(object));

        return this.isAConstant() ? new StringLiteral(this.getValue(object)) : this;
    }

    @Override
    public String getValue(final Object object) throws HPersistException {
        return this.getExpr1().getValue(object) + this.getExpr2().getValue(object);
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr1().isAConstant() && this.getExpr2().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getExpr1().setContext(context);
        this.getExpr2().setContext(context);
    }

}
