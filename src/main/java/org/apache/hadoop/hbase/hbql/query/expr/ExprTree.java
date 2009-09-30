package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.TypeSignature;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.GenericColumn;
import org.apache.hadoop.hbase.hbql.query.schema.VariableAttrib;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ExprTree extends ExprContext implements Serializable {

    private static TypeSignature exprSignature = new TypeSignature(null, BooleanValue.class);

    private final boolean readFromHBaseMap;

    private ExprTree(final boolean readFromHBaseMap, final GenericValue rootValue) {
        super(exprSignature, rootValue);
        this.readFromHBaseMap = readFromHBaseMap;
    }

    public static ExprTree newExprTree(final boolean readFromHBaseMap, final BooleanValue booleanValue) {
        return new ExprTree(readFromHBaseMap, booleanValue);
    }

    public Boolean evaluate(final Object object) throws HBqlException {

        this.validateTypes(true);
        this.optimize();

        // Set it once per evaluation
        DateLiteral.resetNow();

        return (this.getGenericValue(0) == null) || (Boolean)this.getGenericValue(0).getValue(object);
    }

    public void validate(final List<VariableAttrib> attribList) throws HBqlException {

        // Check if all the variables referenced in the where clause are present in the fieldList.
        for (final GenericColumn var : this.getColumnList()) {
            if (!attribList.contains(var.getVariableAttrib()))
                throw new HBqlException("Variable " + var.getVariableName() + " used in where clause but it is not "
                                        + "not in the select list");
        }
    }

    @Override
    public String asString() {
        return this.getGenericValue(0).asString();
    }

    @Override
    public boolean readFromHBaseMap() {
        return this.readFromHBaseMap;
    }
}