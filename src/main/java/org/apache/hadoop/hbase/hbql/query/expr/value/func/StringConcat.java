package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.GenericTwoExprExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 6:20:20 PM
 */
public class StringConcat extends GenericTwoExprExpr<StringValue> implements StringValue {


    public StringConcat(final StringValue expr1, final StringValue expr2) {
        super(expr1, expr2);
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = getExpr1().getExprVariables();
        retval.addAll(this.getExpr2().getExprVariables());
        return retval;
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr1().validateType();
        final Class<? extends ValueExpr> type2 = this.getExpr2().validateType();

        if (!type1.equals(type2))
            throw new HPersistException("Type mismatch in StringConcat");

        if (!ExprTree.isOfType(type1, StringValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in StringConcat");

        return StringValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {

        this.setExpr1((StringValue)this.getExpr1().getOptimizedValue());
        this.setExpr2((StringValue)this.getExpr2().getOptimizedValue());

        return this.isAConstant() ? new StringLiteral(this.getValue(null)) : this;
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
