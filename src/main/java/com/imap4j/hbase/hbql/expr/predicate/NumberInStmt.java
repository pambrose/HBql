package com.imap4j.hbase.hbql.expr.predicate;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbase.HPersistException;
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
public class NumberInStmt extends GenericInStmt<NumberValue> implements PredicateExpr {

    public NumberInStmt(final NumberValue expr, final boolean not, final List<NumberValue> vals) {
        super(not, expr, vals);
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.setExpr(new NumberLiteral(this.getExpr().getValue(object)));
        else
            retval = false;

        if (!this.optimizeList(object))
            retval = false;

        return retval;
    }

    private boolean optimizeList(final Object object) throws HPersistException {

        boolean retval = true;
        final List<NumberValue> newvalList = Lists.newArrayList();

        for (final NumberValue val : this.getValueList()) {
            if (val.optimizeForConstants(object)) {
                newvalList.add(new NumberLiteral(val.getValue(object)));
            }
            else {
                newvalList.add(val);
                retval = false;
            }
        }

        // Swap new values to list
        this.getValueList().clear();
        this.getValueList().addAll(newvalList);

        return retval;

    }

    protected boolean evaluateList(final Object object) throws HPersistException {

        final long attribVal = this.getExpr().getValue(object).longValue();
        for (final NumberValue obj : this.getValueList()) {
            final long val = obj.getValue(object).longValue();
            if (attribVal == val)
                return true;
        }
        return false;
    }
}