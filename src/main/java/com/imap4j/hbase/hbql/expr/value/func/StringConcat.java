package com.imap4j.hbase.hbql.expr.value.func;

import com.google.common.collect.Lists;
import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.node.StringValue;
import com.imap4j.hbase.hbql.expr.value.literal.StringLiteral;
import com.imap4j.hbase.hbql.schema.ExprSchema;

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

    private List<StringValue> getValueList() {
        return this.vals;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = Lists.newArrayList();
        for (final StringValue val : this.getValueList())
            retval.addAll(val.getExprVariables());
        return retval;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (!this.optimizeList(object))
            retval = false;

        return retval;
    }

    @Override
    public String getValue(final Object object) throws HPersistException {

        if (this.getValueList().size() == 1)
            return this.getValueList().get(0).getValue(object);

        final StringBuilder sbuf = new StringBuilder();
        for (final StringValue val : this.getValueList())
            sbuf.append(val.getValue(object));

        return sbuf.toString();
    }

    @Override
    public boolean isAConstant() {
        return this.listIsConstant();
    }

    private boolean optimizeList(final Object object) throws HPersistException {

        boolean retval = true;
        final List<StringValue> newvalList = Lists.newArrayList();

        for (final StringValue val : this.getValueList()) {
            if (val.optimizeForConstants(object)) {
                newvalList.add(new StringLiteral(val.getValue(object)));
            }
            else {
                newvalList.add(val);
                retval = false;
            }
        }

        // Swap new values to list
        this.getValueList().clear();
        this.getValueList().addAll(newvalList);

        return retval;

    }

    private boolean listIsConstant() {

        for (final StringValue val : this.getValueList()) {
            if (!val.isAConstant())
                return false;
        }
        return true;
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        for (final StringValue val : this.getValueList())
            val.setSchema(schema);
    }

}
