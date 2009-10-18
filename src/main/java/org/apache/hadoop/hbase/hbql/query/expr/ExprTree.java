package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;

public class ExprTree extends ExprContext {

    private static TypeSignature exprSignature = new TypeSignature(null, BooleanValue.class);

    private String exprText = null;
    private boolean useHBaseResult = false;

    private ExprTree(final GenericValue rootValue) {
        super(exprSignature, rootValue);
    }

    public static ExprTree newExprTree(final boolean booleanValue) {
        return newExprTree(new BooleanLiteral(booleanValue));
    }

    public static ExprTree newExprTree(final GenericValue booleanValue) {
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

    public void setSchema(final Schema schema) {
        super.setSchema(schema);
        if (schema != null)
            schema.addExprTree(this);
    }

    public String getExprText() {
        return this.exprText;
    }

    public void setExprText(final String exprText) {
        this.exprText = exprText;
    }

    public boolean equals(final Object o) {
        if (o == null || (!(o instanceof ExprTree)))
            return false;

        final String name1 = ((ExprTree)o).getExprText();
        final String name2 = this.getExprText();
        return name1 != null && name2 != null && name1.equals(name2);
    }
}