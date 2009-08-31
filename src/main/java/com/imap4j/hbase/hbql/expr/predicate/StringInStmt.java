package com.imap4j.hbase.hbql.expr.predicate;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringInStmt implements PredicateExpr {

    private StringValue expr = null;
    private final boolean not;
    private final List<StringValue> valList;

    public StringInStmt(final StringValue expr, final boolean not, final List<StringValue> valList) {
        this.expr = expr;
        this.not = not;
        this.valList = valList;
    }

    private StringValue getExpr() {
        return expr;
    }

    private List<StringValue> getVals() {
        return valList;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(context))
            this.expr = new StringLiteral(this.getExpr().getValue(context));
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
        final List<StringValue> newvalList = Lists.newArrayList();

        for (final StringValue num : this.getVals()) {
            if (num.optimizeForConstants(context)) {
                newvalList.add(new StringLiteral(num.getValue(context)));
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

        final String attribVal = this.getExpr().getValue(context);
        for (final StringValue obj : this.getVals()) {
            final String val = obj.getValue(context);
            if (attribVal.equals(val))
                return true;
        }

        return false;
    }
}