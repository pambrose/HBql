package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ExprTree extends ExprContext {

    private static TypeSignature exprSignature = new TypeSignature(null, BooleanValue.class);

    private boolean useHBaseResult = false;

    private ExprTree(final GenericValue rootValue) {
        super(exprSignature, rootValue);
    }

    public static ExprTree newExprTree(final BooleanValue booleanValue) {
        return new ExprTree(booleanValue);
    }

    public Boolean evaluate(final Object object) throws HBqlException {
        return !this.isValid() || (Boolean)this.evaluate(0, true, object);
    }

    public void validate() throws HBqlException {
        // Noop for now
    }

    @Override
    public String asString() {
        return this.isValid() ? this.getGenericValue(0).asString() : "";
    }

    public void setUseHBaseResult(final boolean useHBaseResult) {
        this.useHBaseResult = useHBaseResult;
    }

    @Override
    public boolean useHBaseResult() {
        return this.useHBaseResult;
    }
}