package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.ObjectValue;
import org.apache.expreval.expr.node.StringValue;

public class ObjectLiteral extends GenericLiteral<Object> implements ObjectValue {

    public ObjectLiteral(final Object value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return StringValue.class;
    }

    public String asString() {
        return "\"" + this.getValue(null).toString() + "\"";
    }
}