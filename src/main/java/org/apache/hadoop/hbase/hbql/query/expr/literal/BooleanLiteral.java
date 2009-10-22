package org.apache.hadoop.hbase.hbql.query.expr.literal;

import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class BooleanLiteral extends GenericLiteral<Boolean> implements BooleanValue {

    public BooleanLiteral(final String text) {
        super(text.equalsIgnoreCase("true"));
    }

    public BooleanLiteral(final Boolean value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return BooleanValue.class;
    }
}