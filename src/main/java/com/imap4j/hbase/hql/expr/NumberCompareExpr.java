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

    public ValueExpr expr1;
    public ValueExpr expr2;


    public NumberCompareExpr(final ValueExpr expr1, final Operator op, final ValueExpr expr2) {
        super(op);
        this.expr1 = expr1;
    }

    @Override
    public boolean evaluate(final ClassSchema classSchema, final HPersistable recordObj) throws HPersistException {

        final Number val1 = (Number)expr1.getValue(classSchema, recordObj);
        final Number val2 = (Number)expr2.getValue(classSchema, recordObj);

        switch (this.op) {
            case EQ: {
                return val1 == val2;
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