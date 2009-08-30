package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.PredicateExpr;
import com.imap4j.hbase.hbql.expr.StringValue;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringInStmt implements PredicateExpr {

    private final StringValue expr;
    private final boolean not;
    private final List<StringValue> valList;

    public StringInStmt(final StringValue expr, final boolean not, final List<StringValue> valList) {
        this.expr = expr;
        this.not = not;
        this.valList = valList;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {

        final boolean retval = this.evaluateList(context);
        return (this.not) ? !retval : retval;
    }

    private boolean evaluateList(final AttribContext context) throws HPersistException {

        final String attribVal = this.expr.getValue(context);
        for (final StringValue obj : this.valList) {
            final String val = obj.getValue(context);
            if (attribVal.equals(val))
                return true;
        }

        return false;

    }
}