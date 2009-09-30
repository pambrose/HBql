package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 28, 2009
 * Time: 2:38:26 PM
 */
public abstract class SelectArgs extends ExprContext {

    public static enum Type {

        TIMERANGE(new TypeSignature(null, DateValue.class, DateValue.class)),
        LIMIT(new TypeSignature(null, NumberValue.class)),
        VERSION(new TypeSignature(null, NumberValue.class));

        private final TypeSignature typeSignature;

        Type(final TypeSignature typeSignature) {
            this.typeSignature = typeSignature;
        }

        public TypeSignature getTypeSignature() {
            return typeSignature;
        }
    }

    private final Type type;

    protected SelectArgs(final Type type, final GenericValue... exprs) {
        super(exprs);
        this.type = type;
    }

    private void validateType(final int i) throws HBqlException {

        final GenericValue arg = this.getArg(i);

        if (arg == null)
            throw new HBqlException("Null value invalid");

        final Class<? extends GenericValue> sigarg = this.type.getTypeSignature().getArg(i);

        if (!sigarg.isAssignableFrom(arg.getClass()))
            throw new HBqlException("Invalid type " + this.getArg(i).getClass().getSimpleName());
    }

    public void optimize() throws HBqlException {
        for (int i = 0; i < this.getGenericValues().size(); i++)
            this.setGenericValue(i, this.getGenericValues().get(i).getOptimizedValue());
    }

    protected void setContext() {
        for (final GenericValue val : this.getGenericValues()) {
            try {
                val.setContext(this);
            }
            catch (HBqlException e) {
                e.printStackTrace();
            }
        }
    }

    public void validateTypes() throws HBqlException {

        for (final GenericValue arg : this.getGenericValues())
            arg.validateTypes(null, false);

        for (int i = 0; i < this.type.getTypeSignature().getArgCount(); i++)
            validateType(i);
    }

    public GenericValue getArg(final int i) {
        return this.getGenericValues().get(i);
    }

    abstract public String asString();

}
