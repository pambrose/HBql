package com.imap4j.hbase.hbql.expr.value.func;

import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.StringValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class StringFunction implements StringValue {

    public enum FUNC {
        CONCAT,
        TRIM,
        LOWER,
        UPPER
    }

    private final FUNC func;
    private final StringValue[] expr;

    public StringFunction(final FUNC func, final StringValue... expr) {
        this.func = func;
        this.expr = expr;
    }

    private FUNC getFunc() {
        return func;
    }

    // TODO Deal with this
    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {
        return false;
    }

    @Override
    public String getValue(final EvalContext context) throws HPersistException {

        switch (this.getFunc()) {
            case TRIM: {
                final String val = this.expr[0].getValue(context);
                return val.trim();
            }

            case LOWER: {
                final String val = this.expr[0].getValue(context);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = this.expr[0].getValue(context);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = this.expr[0].getValue(context);
                final String v2 = this.expr[1].getValue(context);
                return v1 + v2;
            }

            default:
                throw new HPersistException("Error in StringFunction.getValue() " + this.getFunc());
        }

    }

}
