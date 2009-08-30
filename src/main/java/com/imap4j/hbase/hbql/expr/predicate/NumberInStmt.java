package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.NumberValue;
import com.imap4j.hbase.hbql.expr.PredicateExpr;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class NumberInStmt implements PredicateExpr {


    private final NumberValue number;
    private final boolean not;
    private final List<Object> valList;

    public NumberInStmt(final NumberValue number, final boolean not, final List<Object> valList) {
        this.number = number;
        this.not = not;
        this.valList = valList;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {

        final boolean retval = this.evaluateList(context);
        return (this.not) ? !retval : retval;
    }

    private boolean evaluateList(final AttribContext context) throws HPersistException {

        final Number number = this.number.getValue(context);
        final int attribVal = number.intValue();
        for (final Object obj : this.valList) {
            final Number numobj = ((NumberValue)obj).getValue(context);
            final int val = numobj.intValue();
            if (attribVal == val)
                return true;
        }
        return false;

    }
}