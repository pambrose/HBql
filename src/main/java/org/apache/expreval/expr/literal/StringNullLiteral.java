package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.StringValue;

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