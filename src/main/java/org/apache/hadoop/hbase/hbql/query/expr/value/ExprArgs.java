package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 27, 2009
 * Time: 9:15:36 PM
 */
public class ExprArgs {

    private final List<GenericValue> genericValues;

    public ExprArgs(final List<GenericValue> genericValues) {
        this.genericValues = genericValues;
    }

    private List<GenericValue> getArgs() {
        return this.genericValues;
    }

    public int size() {
        return this.getArgs().size();
    }

    public GenericValue getArg(final int i) {
        return this.getArgs().get(i);
    }

    public void setArg(final int i, final GenericValue val) {
        this.getArgs().set(i, val);
    }

    public void setContext(final ExprTree context) {
        for (final GenericValue val : this.getArgs())
            val.setContext(context);
    }

    public void optimizeArgs() throws HBqlException {
        for (int i = 0; i < this.size(); i++)
            this.setArg(i, this.getArg(i).getOptimizedValue());
    }

    public boolean isAConstant() throws HBqlException {
        for (final GenericValue val : this.getArgs())
            if (!val.isAConstant())
                return false;
        return true;
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder("(");

        boolean first = true;
        for (final GenericValue val : this.getArgs()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(val.asString());
            first = false;
        }

        sbuf.append(")");

        return sbuf.toString();
    }

}
