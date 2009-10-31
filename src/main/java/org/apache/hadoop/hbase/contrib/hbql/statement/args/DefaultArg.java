package org.apache.hadoop.hbase.contrib.hbql.statement.args;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.TypeSignature;
import org.apache.expreval.expr.node.GenericValue;

import java.io.Serializable;

public class DefaultArg extends MultipleExpressionContext implements Serializable {

    // We have to make vlaue transient because Object is not serializable for hbqlfilter
    // We will compute it again on the server after reset is called
    private transient Object value = null;
    private volatile boolean computed = false;

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

    public boolean useResultData() {
        return false;
    }
}