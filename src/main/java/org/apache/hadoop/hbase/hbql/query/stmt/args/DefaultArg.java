package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.TypeSignature;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

import java.io.Serializable;

public class DefaultArg extends ExprContext implements Serializable {

    // We have to delay computation of value because the resulting Object
    // is not serializable for filter
    private transient Object value = null;
    private boolean computed = false;

    public DefaultArg(final Class<? extends GenericValue> exprType, final GenericValue expr) throws HBqlException {
        super(new TypeSignature(null, exprType), expr);

        // This will force the type checking to happen
        this.getValue();
    }

    public void reset() {
        this.computed = false;
    }

    public Object getValue() throws HBqlException {

        if (!computed) {
            synchronized (this) {
                if (!computed) {
                    // Type checking happens in this call, so we force it above in the constructor
                    this.value = this.evaluateConstant(0, false, null);
                    this.computed = true;
                }
            }
        }

        return this.value;
    }

    public String asString() {
        return this.getGenericValue(0).asString();
    }

    public boolean useHBaseResult() {
        return false;
    }
}