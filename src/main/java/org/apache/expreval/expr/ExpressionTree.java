package org.apache.expreval.expr;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.literal.BooleanLiteral;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;

public class ExpressionTree extends MultipleExpressionContext {

    private static TypeSignature exprSignature = new TypeSignature(null, BooleanValue.class);
    private boolean useResultData = false;

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

    public void setUseResultData(final boolean useResultData) {
        this.useResultData = useResultData;
    }

    public boolean useResultData() {
        return this.useResultData;
    }
}