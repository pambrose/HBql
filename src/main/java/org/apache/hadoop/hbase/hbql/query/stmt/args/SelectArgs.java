package org.apache.hadoop.hbase.hbql.query.stmt.args;

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

    protected SelectArgs(final Type type, final GenericValue... exprs) {
        super(type.getTypeSignature(), exprs);
    }

    @Override
    public boolean useHBaseResult() {
        return false;
    }
}
