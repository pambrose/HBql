package org.apache.hadoop.hbase.hbql.query.expr.value;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 10, 2009
 * Time: 11:33:09 AM
 */
public abstract class GenericExpr implements GenericValue {

    private final TypeSignature typeSignature;
    private final List<GenericValue> argList = Lists.newArrayList();

    protected GenericExpr(final TypeSignature typeSignature, final GenericValue... exprs) {
        this(typeSignature, Arrays.asList(exprs));
    }

    protected GenericExpr(final TypeSignature typeSignature, final List<GenericValue> exprList) {
        this.typeSignature = typeSignature;
        this.argList.addAll(exprList);
    }

    protected GenericExpr(final TypeSignature typeSignature, final GenericValue expr, final List<GenericValue> exprList) {
        this.typeSignature = typeSignature;
        this.argList.add(expr);
        this.argList.addAll(exprList);
    }

    protected List<GenericValue> getArgList() {
        return this.argList;
    }

    protected List<GenericValue> getSubArgs(final int i) {
        return this.getArgList().subList(i, this.getArgList().size());
    }

    public boolean isAConstant() throws HBqlException {
        for (final GenericValue val : this.getArgList())
            if (!val.isAConstant())
                return false;
        return true;
    }

    public void setContext(final ExprTree context) {
        for (final GenericValue val : this.getArgList())
            val.setContext(context);
    }

    public void optimizeArgs() throws HBqlException {
        for (int i = 0; i < this.getArgList().size(); i++)
            this.setArg(i, this.getArg(i).getOptimizedValue());
    }

    public GenericValue getArg(final int i) {
        return this.getArgList().get(i);
    }

    public void setArg(final int i, final GenericValue val) {
        this.getArgList().set(i, val);
    }

    protected TypeSignature getTypeSignature() {
        return this.typeSignature;
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

    public void validateParentClass(final Class<? extends GenericValue> parentClazz,
                                    final Class<? extends GenericValue>... clazzes) throws TypeException {

        List<Class<? extends GenericValue>> classList = null;

        for (final Class<? extends GenericValue> clazz : clazzes) {

            if (clazz == null)
                continue;

            if (!parentClazz.isAssignableFrom(clazz)) {
                if (classList == null)
                    classList = Lists.newArrayList();
                classList.add(clazz);
            }
        }

        if (classList != null) {
            final StringBuilder sbuf = new StringBuilder("Expecting type " + parentClazz.getSimpleName()
                                                         + " but encountered type"
                                                         + ((classList.size() > 1) ? "s" : "") + ": ");
            boolean first = true;
            for (final Class clazz : classList) {
                if (!first)
                    sbuf.append(", ");
                sbuf.append(clazz.getSimpleName());
                first = false;
            }

            sbuf.append(" in expression " + this.asString());

            throw new TypeException(sbuf.toString());
        }
    }

}
