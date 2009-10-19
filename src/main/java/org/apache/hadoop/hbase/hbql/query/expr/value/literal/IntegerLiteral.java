package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.IntegerValue;

public class IntegerLiteral extends GenericLiteral<Integer> implements IntegerValue {

    public IntegerLiteral(final Integer value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return IntegerValue.class;
    }
}