package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.query.expr.node.FloatValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class FloatLiteral extends GenericLiteral<Float> implements FloatValue {

    public FloatLiteral(final Float value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return FloatValue.class;
    }
}