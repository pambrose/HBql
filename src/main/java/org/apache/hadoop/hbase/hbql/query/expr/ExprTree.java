package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.expr.value.var.GenericColumn;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ExprTree extends ExprContext implements Serializable {

    private ExprTree() {
    }

    public static ExprTree newExprTree(final BooleanValue booleanValue) {
        final ExprTree tree = new ExprTree();
        tree.setGenericValue(booleanValue);
        return tree;
    }

    public String asString() {
        return this.getGenericValue().asString();
    }

    public Boolean evaluate(final Object object) throws HBqlException {

        this.validateTypes();
        this.optimize();

        // Set it once per evaluation
        DateLiteral.resetNow();

        final boolean retval = (this.getGenericValue() == null) || (Boolean)this.getGenericValue().getValue(object);

        return retval;
    }

    public void setSchema(final Schema schema, final List<String> fieldList) throws HBqlException {

        if (schema == null)
            return;

        if (this.isValid()) {

            this.setSchema(schema);

            // Check if all the variables referenced in the where clause are present in the fieldList.
            final List<String> selectList = schema.getAliasAndQualifiedNameFieldList(fieldList);

            for (final GenericColumn var : this.getColumnList()) {
                if (!selectList.contains(var.getVariableName()))
                    throw new HBqlException("Variable " + var.getVariableName() + " used in where clause but it is not "
                                            + "not in the select list");
            }
        }
    }

}