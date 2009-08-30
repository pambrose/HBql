package com.imap4j.hbase.hbql.expr.predicate;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.NumberValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class NumberCompare extends CompareExpr {

    private final NumberValue expr1;
    private final NumberValue expr2;

    public NumberCompare(final NumberValue expr1, final Operator op, final NumberValue expr2) {
        super(op);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public boolean evaluate(final AttribContext context) throws HPersistException {

        final int val1 = this.expr1.getValue(context).intValue();
        final int val2 = this.expr2.getValue(context).intValue();

        switch (this.getOperator()) {
            case EQ:
                return val1 == val2;
            case GT:
                return val1 > val2;
            case GTEQ:
                return val1 >= val2;
            case LT:
                return val1 < val2;
            case LTEQ:
                return val1 <= val2;
            case NOTEQ:
                return val1 != val2;
        }

        throw new HPersistException("Error in NumberCompareExpr.evaluate()");
    }

}