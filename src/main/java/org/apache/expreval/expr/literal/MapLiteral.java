package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.MapValue;

import java.util.Map;

public class MapLiteral extends GenericLiteral<Map> implements MapValue {

    public MapLiteral(final Map value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return MapValue.class;
    }

    public String asString() {
        return "\"" + this.getValue(null) + "\"";
    }
}