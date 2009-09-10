package com.imap4j.hbase.hbql.expr.value.func;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.EvalContext;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 29, 2009
 * Time: 6:20:20 PM
 */
public class StringConcat implements StringValue {

    private final List<StringValue> vals;

    public StringConcat(final List<StringValue> vals) {
        this.vals = vals;
    }

    private List<StringValue> getValList() {
        return this.vals;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = Lists.newArrayList();
        for (final StringValue val : this.getValList())
            retval.addAll(val.getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final EvalContext context) throws HPersistException {

        boolean retval = true;

        if (!this.optimizeList(context))
            retval = false;

        return retval;
    }

    @Override
    public String getValue(final EvalContext context) throws HPersistException {

        if (this.getValList().size() == 1)
            return this.getValList().get(0).getValue(context);

        final StringBuilder sbuf = new StringBuilder();
        for (final StringValue val : this.getValList())
            sbuf.append(val.getValue(context));

        return sbuf.toString();
    }

    @Override
    public boolean isAConstant() {
        return this.listIsConstant();
    }

    private boolean optimizeList(final EvalContext context) throws HPersistException {

        boolean retval = true;
        final List<StringValue> newvalList = Lists.newArrayList();

        for (final StringValue val : this.getValList()) {
            if (val.optimizeForConstants(context)) {
                newvalList.add(new StringLiteral(val.getValue(context)));
            }
            else {
                newvalList.add(val);
                retval = false;
            }
        }

        // Swap new values to list
        this.getValList().clear();
        this.getValList().addAll(newvalList);

        return retval;

    }

    private boolean listIsConstant() {

        for (final StringValue val : this.getValList()) {
            if (!val.isAConstant())
                return false;
        }
        return true;
    }

}
