package org.apache.hadoop.hbase.hbql.stmt.expr.literal;

import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.StringValue;

public class StringLiteral extends GenericLiteral<String> implements StringValue {

    public StringLiteral(final String value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return StringValue.class;
    }

    public String asString() {
        return "\"" + this.getValue(null) + "\"";
    }
}