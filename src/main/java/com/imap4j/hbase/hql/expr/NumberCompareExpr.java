package com.imap4j.hbase.hql.expr;

import com.imap4j.hbase.hql.ClassSchema;
import com.imap4j.hbase.hql.HPersistException;
import com.imap4j.hbase.hql.HPersistable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 10:30:32 PM
 */
public class NumberCompareExpr extends CompareExpr {

    private final ValueExpr expr1;
    private final ValueExpr expr2;

    public NumberCompareExpr(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(op);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {

        final int val1 = ((Number)expr1.getValue(classSchema, recordObj)).intValue();
        final int val2 = ((Number)expr2.getValue(classSchema, recordObj)).intValue();

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
            case LTGT:
                return val1 != val2;
        }

        throw new HPersistException("Error in NumberCompareExpr.evaluate()");
    }

}