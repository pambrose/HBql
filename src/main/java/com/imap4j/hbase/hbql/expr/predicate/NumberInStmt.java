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
public class NumberInStmt extends GenericInStmt implements PredicateExpr {

    private NumberValue expr = null;
    private final List<NumberValue> vals;

    public NumberInStmt(final NumberValue expr, final boolean not, final List<NumberValue> vals) {
        super(not);
        this.expr = expr;
        this.vals = vals;
    }

    protected NumberValue getExpr() {
        return this.expr;
    }

    private List<NumberValue> getVals() {
        return this.vals;
    }

    @Override
    public List<String> getQualifiedColumnNames() {
        final List<String> retval = this.getExpr().getQualifiedColumnNames();
        for (final NumberValue val : this.getVals())
            retval.addAll(val.getQualifiedColumnNames());
        return retval;
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
        return (this.isNot()) ? !retval : retval;
    }

    private boolean optimizeList(final EvalContext context) throws HPersistException {

        boolean retval = true;
        final List<NumberValue> newvalList = Lists.newArrayList();

        for (final NumberValue val : this.getVals()) {
            if (val.optimizeForConstants(context)) {
                newvalList.add(new NumberLiteral(val.getValue(context)));
            }
            else {
                newvalList.add(val);
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