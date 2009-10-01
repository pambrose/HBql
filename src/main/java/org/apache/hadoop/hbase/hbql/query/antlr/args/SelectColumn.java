package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 28, 2009
 * Time: 5:31:29 PM
 */
public class SelectColumn extends ExprContext {

    public enum Type {
        ALLTABLECOLUMNS, ALLFAMILYCOLUMNS, GENERICEXPR
    }

    private final Type type;
    private final String familyName;
    private final String asName;
    private Object evaluationValue = null;

    private SelectColumn(final Type type,
                         final String familyName,
                         final String asName,
                         final GenericValue genericValue) {
        super(null, genericValue);
        this.type = type;
        this.asName = asName;
        this.familyName = (familyName == null) ? null : familyName.replace(" ", "").replace(":*", "");
    }

    public static SelectColumn newAllColumns() {
        return new SelectColumn(Type.ALLTABLECOLUMNS, null, null, null);
    }

    public static SelectColumn newFamilyColumns(final String family) {
        return new SelectColumn(Type.ALLFAMILYCOLUMNS, family, null, null);
    }

    public static SelectColumn newColumn(final GenericValue expr, final String as) {
        return new SelectColumn(Type.GENERICEXPR, null, as, expr);
    }

    public Type getType() {
        return this.type;
    }

    public String getFamilyName() {
        return this.familyName;
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
        this.evaluationValue = this.getGenericValue().getValue(result);
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
