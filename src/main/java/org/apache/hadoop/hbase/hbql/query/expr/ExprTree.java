package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

public class ExprTree extends ExprContext {

    private static TypeSignature exprSignature = new TypeSignature(null, BooleanValue.class);

    private boolean useHBaseResult = false;

    private ExprTree(final GenericValue rootValue) {
        super(exprSignature, rootValue);
    }

    public static ExprTree newExprTree(final boolean booleanValue) {
        return newExprTree(new BooleanLiteral(booleanValue));
    }

    public static ExprTree newExprTree(final BooleanValue booleanValue) {
        if (booleanValue == null)
            return new ExprTree(new BooleanLiteral(true));
        else
            return new ExprTree(booleanValue);
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