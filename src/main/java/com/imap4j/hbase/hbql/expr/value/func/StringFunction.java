package com.imap4j.hbase.hbql.expr.value.func;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.predicate.GenericFunction;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 5:28:17 PM
 */
public class StringFunction extends GenericFunction implements StringValue {

    private final StringValue[] exprs;

    public StringFunction(final FUNC func, final StringValue... exprs) {
        super(func);
        this.exprs = exprs;
    }

    private StringValue[] getExprs() {
        return exprs;
    }

    @Override
    public List<String> getAttribNames() {
        final List<String> retval = Lists.newArrayList();
        for (final StringValue val : this.getExprs())
            retval.addAll(val.getAttribNames());
        return retval;
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
                final String val = this.getExprs()[0].getValue(context);
                return val.trim();
            }

            case LOWER: {
                final String val = this.getExprs()[0].getValue(context);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = this.getExprs()[0].getValue(context);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = this.getExprs()[0].getValue(context);
                final String v2 = this.getExprs()[1].getValue(context);
                return v1 + v2;
            }

            default:
                throw new HPersistException("Error in StringFunction.getValue() " + this.getFunc());
        }

    }

}
