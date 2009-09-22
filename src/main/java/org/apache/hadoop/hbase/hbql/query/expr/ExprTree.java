package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.DateLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ExprTree implements Serializable {

    private Schema schema = null;
    private BooleanValue treeRoot = null;
    private long start, end;
    private boolean optimized = false;

    private ExprTree(final BooleanValue treeRoot) {
        this.treeRoot = treeRoot;
    }

    public static ExprTree newExprTree(final BooleanValue booleanValue) {
        return new ExprTree(booleanValue);
    }

    private BooleanValue getTreeRoot() {
        return this.treeRoot;
    }

    private void setTreeRoot(final BooleanValue treeRoot) {
        this.treeRoot = treeRoot;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public boolean isValid() {
        return this.getTreeRoot() != null;
    }

    public void setSchema(final Schema schema) {
        if (schema != null) {
            this.schema = schema;
            this.getTreeRoot().setContext(this);
        }
    }

    public void optimize() throws HPersistException {
        this.setTreeRoot((BooleanValue)this.getTreeRoot().getOptimizedValue());
        this.optimized = true;
    }

    public List<ExprVariable> getExprVariables() {
        if (this.getTreeRoot() == null)
            return Lists.newArrayList();
        else
            return this.getTreeRoot().getExprVariables();
    }

    public Boolean evaluate(final Object object) throws HPersistException {

        this.start = System.nanoTime();

        // Set it once per evaluation
        DateLiteral.resetNow();

        final boolean retval = (this.getTreeRoot() == null) || (this.getTreeRoot().getValue(object));

        this.end = System.nanoTime();

        return retval;
    }

    public long getElapsedNanos() {
        return this.end - this.start;
    }

    public void setSchema(final Schema schema, final List<String> fieldList) throws HPersistException {

        if (this.isValid()) {
            this.setSchema(schema);
            this.optimize();

            // Check if all the variables referenced in the where clause are present in the fieldList.
            final List<String> selectList = schema.getAliasAndQualifiedNameFieldList(fieldList);

            final List<ExprVariable> referencedVars = this.getExprVariables();
            for (final ExprVariable var : referencedVars) {
                if (!selectList.contains(var.getName()))
                    throw new HPersistException("Variable " + var.getName() + " used in where clause but it is not "
                                                + "not in the select list");
            }
        }
    }
}