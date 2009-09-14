package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.node.PredicateExpr;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;
import com.imap4j.hbase.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringInStmt extends GenericInStmt<StringValue> implements PredicateExpr {

    public StringInStmt(final StringValue expr, final boolean not, final List<StringValue> valList) {
        super(not, expr, valList);
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.setExpr(new StringLiteral(this.getExpr().getCurrentValue(object)));
        else
            retval = false;

        if (!this.optimizeList(object))
            retval = false;

        return retval;
    }

    private boolean optimizeList(final Object object) throws HPersistException {

        boolean retval = true;
        final List<StringValue> newvalList = Lists.newArrayList();

        for (final StringValue val : this.getValueList()) {
            if (val.optimizeForConstants(object)) {
                newvalList.add(new StringLiteral(val.getCurrentValue(object)));
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

        final String attribVal = this.getExpr().getCurrentValue(object);
        for (final StringValue obj : this.getValueList()) {
            final String val = obj.getCurrentValue(object);
            if (attribVal.equals(val))
                return true;
        }

        return false;
    }

}