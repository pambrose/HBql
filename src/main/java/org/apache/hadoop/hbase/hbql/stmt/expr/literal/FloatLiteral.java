package org.apache.hadoop.hbase.hbql.stmt.expr.literal;

import org.apache.hadoop.hbase.hbql.stmt.expr.node.FloatValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class FloatLiteral extends GenericLiteral<Float> implements FloatValue {

    public FloatLiteral(final Float value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return FloatValue.class;
    }
}