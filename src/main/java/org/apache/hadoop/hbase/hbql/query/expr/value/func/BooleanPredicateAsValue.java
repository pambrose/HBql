package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.PredicateExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 2:35:57 PM
 */
public class BooleanPredicateAsValue implements BooleanValue {

    private PredicateExpr pred = null;

    public BooleanPredicateAsValue(final PredicateExpr pred) {
        this.pred = pred;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getPred().optimizeForConstants(object))
            this.setPred(new BooleanLiteral(this.getPred().evaluate(object)));
        else
            retval = false;

        return retval;
    }

    public PredicateExpr getPred() {
        return pred;
    }

    public void setPred(final PredicateExpr pred) {
        this.pred = pred;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return this.getPred().getExprVariables();
    }

    @Override
    public boolean isAConstant() {
        return this.getPred().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getPred().setContext(context);
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {
        return this.getPred().evaluate(object);
    }
}