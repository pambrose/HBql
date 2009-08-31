package com.imap4j.hbase.hbql.expr.predicate;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.NumberValue;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.value.literal.NumberLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberInStmt implements PredicateExpr {

    private NumberValue expr = null;
    private final boolean not;
    private final List<NumberValue> vals;

    public NumberInStmt(final NumberValue expr, final boolean not, final List<NumberValue> vals) {
        this.expr = expr;
        this.not = not;
        this.vals = vals;
    }

    private NumberValue getExpr() {
        return this.expr;
    }

    private List<NumberValue> getVals() {
        return this.vals;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new NumberLiteral(this.getExpr().getValue(context));
        else
            retval = false;

        if (!this.optimizeList(context))
            retval = false;

        return retval;
    }

    @Override
    public boolean evaluate(final EvalContext context) throws HPersistException {
        final boolean retval = this.evaluateList(context);
        return (this.not) ? !retval : retval;
    }

    private boolean optimizeList(final EvalContext context) throws HPersistException {

        boolean retval = true;
        final List<NumberValue> newvalList = Lists.newArrayList();

        for (final NumberValue num : this.getVals()) {
            if (num.optimizeForConstants(context)) {
                newvalList.add(new NumberLiteral(num.getValue(context)));
            }
            else {
                newvalList.add(num);
                retval = false;
            }
        }

        // Swap new values to list
        this.getVals().clear();
        this.getVals().addAll(newvalList);

        return retval;

    }

    private boolean evaluateList(final EvalContext context) throws HPersistException {

        final int attribVal = this.getExpr().getValue(context).intValue();
        for (final NumberValue obj : this.getVals()) {
            final int val = obj.getValue(context).intValue();
            if (attribVal == val)
                return true;
        }
        return false;
    }
}