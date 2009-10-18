package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

public class DefaultArg extends ExprContext {

    public DefaultArg(final Class<? extends GenericValue> exprType, final GenericValue expr) {
        super(new TypeSignature(null, exprType), expr);
    }

    public Object getValue() throws HBqlException {
        return (this.evaluateWithoutColumns(0, false, null));
    }

    public String asString() {
        return this.getGenericValue(0).asString();
    }

    public boolean useHBaseResult() {
        return false;
    }
}