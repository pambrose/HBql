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

    private final TypeSignature typeSignature;
    private final GenericValue[] genericValues;

    public ExprArgs(final TypeSignature typeSignature, final GenericValue... genericValues) {
        this.typeSignature = typeSignature;
        this.genericValues = genericValues;
    }

    public Class<? extends GenericValue> getSignatureReturnType() {
        return getTypeSignature().getSignatureReturnType();
    }

    public List<Class<? extends GenericValue>> getSignatureArgs() {
        return this.getTypeSignature().getSignatureArgs();
    }

    public TypeSignature getTypeSignature() {
        return this.typeSignature;
    }

    private GenericValue[] getArgs() {
        return this.genericValues;
    }

    public int size() {
        return this.getArgs().length;
    }

    public GenericValue getArg(final int i) {
        return this.getArgs()[i];
    }

    public void setContext(final ExprTree context) {
        for (final GenericValue val : this.getArgs())
            val.setContext(context);
    }

    public void optimizeArgs() throws HBqlException {
        for (int i = 0; i < this.size(); i++)
            this.getArgs()[i] = this.getArg(i).getOptimizedValue();
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
