package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.NumberValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 28, 2009
 * Time: 2:38:26 PM
 */
public abstract class SelectArgs {

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
    private final List<GenericValue> argList = Lists.newArrayList();

    protected SelectArgs(final Type type, final GenericValue... exprs) {
        this.type = type;
        this.argList.addAll(Arrays.asList(exprs));
    }

    public List<GenericValue> getArgList() {
        return argList;
    }

    public void validateType(final int i) throws HBqlException {

        if (this.getArg(i) == null)
            throw new HBqlException("Null value invalid");

        if (!this.type.getTypeSignature().getArg(i).isAssignableFrom(this.getArg(i).getClass()))
            throw new HBqlException("Invalid type " + this.getArg(i).getClass().getSimpleName());
    }

    public void validateTypes() throws HBqlException {
        for (int i = 0; i < this.type.getTypeSignature().getArgCount(); i++)
            validateType(i);
    }

    public boolean isValid() {
        if (this.getArgList().size() == 0)
            return false;

        for (final GenericValue val : this.getArgList())
            if (val == null)
                return false;
        return true;
    }

    public GenericValue getArg(final int i) {
        return this.getArgList().get(i);
    }

    abstract public String asString();

}
