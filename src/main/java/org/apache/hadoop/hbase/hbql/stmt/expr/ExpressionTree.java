package org.apache.hadoop.hbase.hbql.stmt.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class ExpressionTree extends ExpressionContext {

    private static TypeSignature exprSignature = new TypeSignature(null, BooleanValue.class);
    private boolean useHBaseResult = false;

    private ExpressionTree(final GenericValue rootValue) {
        super(exprSignature, rootValue);
    }

    public static ExpressionTree newExpressionTree(final boolean booleanValue) {
        return newExpressionTree(new BooleanLiteral(booleanValue));
    }

    public static ExpressionTree newExpressionTree(final GenericValue booleanValue) {
        return new ExpressionTree(booleanValue == null ? new BooleanLiteral(true) : booleanValue);
    }

    public Boolean evaluate(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)this.evaluate(0, true, false, object);
    }

    public String asString() {
        return this.getGenericValue(0).asString();
    }

    public void setUseHBaseResult(final boolean useHBaseResult) {
        this.useHBaseResult = useHBaseResult;
    }

    public boolean useHBaseResult() {
        return this.useHBaseResult;
    }
}