package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.StringValue;

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