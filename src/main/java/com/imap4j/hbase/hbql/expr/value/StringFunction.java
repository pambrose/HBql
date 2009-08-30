package com.imap4j.hbase.hbql.expr.value;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.AttribContext;
import com.imap4j.hbase.hbql.expr.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class StringFunction implements ValueExpr {

    public enum FUNC {
        CONCAT,
        SUBSTRING,
        TRIM,
        LOWER,
        UPPER
    }

    private final FUNC func;
    private final ValueExpr[] expr;

    public StringFunction(final FUNC func, final ValueExpr... expr) {
        this.func = func;
        this.expr = expr;
    }

    private FUNC getFunc() {
        return func;
    }

    @Override
    public Object getValue(final AttribContext context) throws HPersistException {

        switch (this.getFunc()) {
            case TRIM: {
                final String val = (String)this.expr[0].getValue(context);
                return val.trim();
            }

            case LOWER: {
                final String val = (String)this.expr[0].getValue(context);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = (String)this.expr[0].getValue(context);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = (String)this.expr[0].getValue(context);
                final String v2 = (String)this.expr[1].getValue(context);
                return v1 + v2;
            }

            case SUBSTRING: {
                final String val = (String)this.expr[0].getValue(context);
                final int begin = (Integer)this.expr[1].getValue(context);
                final int end = (Integer)this.expr[2].getValue(context);
                return val.substring(begin, end);
            }

            default:
                throw new HPersistException("Error in StringFunction.getValue() " + this.getFunc());
        }

    }

}
