package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.PredicateExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 8:28:06 PM
 */
public class CondFactor implements PredicateExpr {

    private final boolean not;
    private PredicateExpr expr = null;

    public CondFactor(final boolean not, final PredicateExpr expr) {
        this.not = not;
        this.expr = expr;
    }

    private PredicateExpr getExpr() {
        return this.expr;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        return this.getExpr().getExprVariables();
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new BooleanLiteral(this.getExpr().evaluate(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {
        final boolean retval = this.getExpr().evaluate(object);
        return (this.not) ? !retval : retval;

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
