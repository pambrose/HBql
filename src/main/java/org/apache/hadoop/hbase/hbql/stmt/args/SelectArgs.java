package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.hbql.stmt.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.stmt.expr.TypeSignature;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.LongValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.StringValue;

public abstract class SelectArgs extends ExprContext {

    public static enum Type {

        NOARGSKEY(new TypeSignature(null)),
        SINGLEKEY(new TypeSignature(null, StringValue.class)),
        KEYRANGE(new TypeSignature(null, StringValue.class, StringValue.class)),
        TIMERANGE(new TypeSignature(null, DateValue.class, DateValue.class)),
        LIMIT(new TypeSignature(null, LongValue.class)),
        VERSION(new TypeSignature(null, LongValue.class));

        private final TypeSignature typeSignature;

        Type(final TypeSignature typeSignature) {
            this.typeSignature = typeSignature;
        }

        public TypeSignature getTypeSignature() {
            return typeSignature;
        }
    }

    protected SelectArgs(final Type type, final GenericValue... exprs) {
        super(type.getTypeSignature(), exprs);
    }

    public boolean useHBaseResult() {
        return false;
    }
}
