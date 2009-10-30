package org.apache.hadoop.hbase.contrib.hbql.statement.args;

import org.apache.expreval.expr.ExpressionContext;
import org.apache.expreval.expr.TypeSignature;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.LongValue;
import org.apache.expreval.expr.node.StringValue;

public abstract class SelectArgs extends ExpressionContext {

    public static enum Type {

        NOARGSKEY(new TypeSignature(null)),
        SINGLEKEY(new TypeSignature(null, StringValue.class)),
        KEYRANGE(new TypeSignature(null, StringValue.class, StringValue.class)),
        TIMESTAMPRANGE(new TypeSignature(null, DateValue.class, DateValue.class)),
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

    public boolean useResultData() {
        return false;
    }
}
