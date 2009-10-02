package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 1, 2009
 * Time: 8:39:47 PM
 */
public class SelectExprElement extends ExprContext implements SelectElement {

    private final String asName;

    private Object evaluationValue = null;

    public SelectExprElement(final GenericValue genericValue, final String asName) {
        super(null, genericValue);
        this.asName = (asName != null) ? asName : genericValue.asString();
    }

    public static SelectExprElement newExprElement(final GenericValue expr, final String as) {
        return new SelectExprElement(expr, as);
    }

    public String getAsName() {
        return this.asName;
    }

    public GenericValue getGenericValue() {
        return this.getGenericValue(0);
    }

    public Object getEvaluationValue() {
        return evaluationValue;
    }

    public void evaluate(final Result result) throws HBqlException {
        this.validateTypes(true);
        this.optimize();
        this.evaluationValue = this.getGenericValue(0).getValue(result);
    }

    @Override
    public void processSelectElement(final HBaseSchema schema, final List<ColumnAttrib> selectAttribList) {
        this.setSchema(schema);
        selectAttribList.addAll(this.getFamilyQualifiedColumnNameList());
    }

    @Override
    public String asString() {
        return this.getGenericValue(0).asString();
    }

    @Override
    public boolean useHBaseResult() {
        return true;
    }

}
