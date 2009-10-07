package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;

public class StringNullLiteral extends GenericLiteral implements StringValue {

    public StringNullLiteral() {
    }

    public String getValue(final Object object) {
        return null;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowsCollections) throws TypeException {
        return StringValue.class;
    }

    public String asString() {
        return "NULL";
    }
}