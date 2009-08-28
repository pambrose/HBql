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
public class StringCompare extends CompareExpr {

    private final Value expr1;
    private final Value expr2;

    public StringCompare(final Value expr1, final Operator op, final Value expr2) {
        super(op);
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {

        final String val1 = (String)expr1.getValue(classSchema, recordObj);
        final String val2 = (String)expr2.getValue(classSchema, recordObj);

        switch (this.getOperator()) {
            case EQ: {
                return val1.equals(val2);
            }
            case GT: {

            }
            case GTEQ: {

            }
            case LT: {

            }
            case LTEQ: {

            }
            case LTGT: {

            }
        }

        throw new HPersistException("Error in StringCompareExpr.evaluate()");
    }

}
