package org.apache.hadoop.hbase.hbql.query.antlr.args;

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
        ALLTABLECOLUMNS, ALLFAMILYCOLUMNS, GENERICEXPR;
    }

    private final Type type;
    private final String familyName;
    private final GenericValue genericValue;

    private SelectColumn(final Type type, final String familyName, final GenericValue genericValue) {
        this.type = type;
        this.familyName = (familyName == null) ? null : familyName.replace(" ", "").replace(":*", "");
        this.genericValue = genericValue;
    }

    public static SelectColumn newAllColumns() {
        return new SelectColumn(Type.ALLTABLECOLUMNS, null, null);
    }

    public static SelectColumn newFamilyColumns(final String family) {
        return new SelectColumn(Type.ALLFAMILYCOLUMNS, family, null);
    }

    public static SelectColumn newColumn(final GenericValue expr) {
        return new SelectColumn(Type.GENERICEXPR, null, expr);
    }

    public Type getType() {
        return this.type;
    }

    public String getFamilyName() {
        return this.familyName;
    }

    public GenericValue getGenericValue() {
        return genericValue;
    }

}
