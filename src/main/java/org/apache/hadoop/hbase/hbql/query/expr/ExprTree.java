package org.apache.hadoop.hbase.hbql.query.expr;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.PredicateExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;
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
    private PredicateExpr predicateExpr = null;
    private long start, end;
    private boolean optimized = false;

    private ExprTree(final PredicateExpr predicateExpr) {
        this.predicateExpr = predicateExpr;
    }

    public static ExprTree newExprTree(final PredicateExpr expr) {
        return new ExprTree(expr);
    }

    private PredicateExpr getPredicateExpr() {
        return this.predicateExpr;
    }

    private void setPredicateExpr(final PredicateExpr predicateExpr) {
        this.predicateExpr = predicateExpr;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public boolean isValid() {
        return this.getPredicateExpr() != null;
    }

    public void setSchema(final Schema schema) {
        if (schema != null) {
            this.schema = schema;
            this.getPredicateExpr().setSchema(schema);
        }
    }

    public void optimize() throws HPersistException {
        this.optimizeForConstants(null);
        this.optimized = true;
    }

    public List<ExprVariable> getExprVariables() {
        if (this.getPredicateExpr() == null)
            return Lists.newArrayList();
        else
            return this.getPredicateExpr().getExprVariables();
    }

    private boolean optimizeForConstants(final Object object) throws HPersistException {

        boolean retval = true;

        if (this.getPredicateExpr().optimizeForConstants(object))
            this.setPredicateExpr(new BooleanLiteral(this.getPredicateExpr().evaluate(object)));
        else
            retval = false;

        return retval;
    }

    public Boolean evaluate(final Object object) throws HPersistException {

        this.start = System.nanoTime();

        // Set it once per evaluation
        DateLiteral.resetNow();

        final boolean retval = (this.getPredicateExpr() == null) || (this.getPredicateExpr().evaluate(object));

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