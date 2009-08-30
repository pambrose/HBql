package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class StringCompare extends CompareExpr {

    private final StringValue expr1;
    private final StringValue expr2;

    public StringCompare(final StringValue expr1, final Operator op, final StringValue expr2) {
        super(op);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {

        final String val1 = this.expr1.getValue(context);
        final String val2 = this.expr2.getValue(context);

        switch (this.getOperator()) {
            case EQ:
                return val1.equals(val2);
            case NOTEQ:
                return !val1.equals(val2);
            case GT:
                return val1.compareTo(val2) > 0;
            case GTEQ:
                return val1.compareTo(val2) >= 0;
            case LT:
                return val1.compareTo(val2) < 0;
            case LTEQ:
                return val1.compareTo(val2) <= 0;
        }

        throw new HPersistException("Error in StringCompare.evaluate()");
    }

}
