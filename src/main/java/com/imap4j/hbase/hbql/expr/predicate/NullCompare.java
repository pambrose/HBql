package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.PredicateExpr;
import com.imap4j.hbase.hbql.expr.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class NullCompare implements PredicateExpr {

    private final ValueExpr expr;
    private final boolean not;

    public NullCompare(final boolean not, final ValueExpr expr) {
        this.not = not;
        this.expr = expr;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {
        final String val = (String)expr.getValue(context);
        final boolean retval = (val == null);
        return (this.not) ? !retval : retval;
    }

}