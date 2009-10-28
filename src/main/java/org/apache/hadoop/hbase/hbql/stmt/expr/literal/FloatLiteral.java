package org.apache.hadoop.hbase.hbql.stmt.expr.literal;

import org.apache.hadoop.hbase.hbql.stmt.expr.node.FloatValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class FloatLiteral extends GenericLiteral<Float> implements FloatValue {

    public FloatLiteral(final String value) {
        super(Float.valueOf(value.endsWith("F") ? value.substring(0, value.length() - 1) : value));
    }

    public FloatLiteral(final Float value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return FloatValue.class;
    }
}