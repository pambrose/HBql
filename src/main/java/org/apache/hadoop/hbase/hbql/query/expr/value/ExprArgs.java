package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 27, 2009
 * Time: 9:15:36 PM
 */
public class ExprArgs {

    private final List<GenericValue> argList = Lists.newArrayList();

    public ExprArgs(final List<GenericValue> exprList) {
        this.argList.addAll(exprList);
    }

    public ExprArgs(final GenericValue expr, final List<GenericValue> exprList) {
        this.argList.add(expr);
        this.argList.addAll(exprList);
    }

    private List<GenericValue> getArgList() {
        return this.argList;
    }

    public List<GenericValue> getSubArgList(final int i) {
        return this.getArgList().subList(i, this.getArgList().size());
    }

    public int size() {
        return this.getArgList().size();
    }

    public GenericValue getArg(final int i) {
        return this.getArgList().get(i);
    }

    public void setArg(final int i, final GenericValue val) {
        this.getArgList().set(i, val);
    }

    public void setContext(final ExprTree context) {
        for (final GenericValue val : this.getArgList())
            val.setContext(context);
    }

    public void optimizeArgs() throws HBqlException {
        for (int i = 0; i < this.size(); i++)
            this.setArg(i, this.getArg(i).getOptimizedValue());
    }

    public boolean isAConstant() throws HBqlException {
        for (final GenericValue val : this.getArgList())
            if (!val.isAConstant())
                return false;
        return true;
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder("(");

        boolean first = true;
        for (final GenericValue val : this.getArgList()) {
            if (!first)
                sbuf.append(", ");
            sbuf.append(val.asString());
            first = false;
        }

        sbuf.append(")");

        return sbuf.toString();
    }
}
