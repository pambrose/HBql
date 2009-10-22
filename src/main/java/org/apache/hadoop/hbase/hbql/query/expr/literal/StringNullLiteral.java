package org.apache.hadoop.hbase.hbql.query.expr.literal;

import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

public class StringNullLiteral extends GenericLiteral<String> implements StringValue {

    public StringNullLiteral() {
        super(null);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return StringValue.class;
    }

    public String asString() {
        return "NULL";
    }
}