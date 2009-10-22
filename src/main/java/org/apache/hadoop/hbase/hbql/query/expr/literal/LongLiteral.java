package org.apache.hadoop.hbase.hbql.query.expr.literal;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.LongValue;

public class LongLiteral extends GenericLiteral<Long> implements LongValue {

    public LongLiteral(final Long value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return LongValue.class;
    }
}