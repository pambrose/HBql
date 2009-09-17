package org.apache.hadoop.hbase.hbql.query.expr.predicate;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.PredicateExpr;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;
import org.apache.hadoop.hbase.hbql.query.schema.ExprSchema;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringLikeStmt extends GenericNotStmt implements PredicateExpr {

    private StringValue expr = null;
    private StringValue patternExpr = null;

    private Pattern pattern = null;

    public StringLikeStmt(final StringValue expr, final boolean not, final StringValue patternExpr) {
        super(not);
        this.expr = expr;
        this.patternExpr = patternExpr;
    }

    private StringValue getExpr() {
        return this.expr;
    }

    private StringValue getPatternExpr() {
        return this.patternExpr;
    }

    private Pattern getPattern() {
        return this.pattern;
    }

    @Override
    public Boolean evaluate(final Object object) throws HPersistException {

        if (this.getPatternExpr().isAConstant()) {
            if (this.pattern == null) {
                final String pattern = this.getPatternExpr().getCurrentValue(object);
                this.pattern = Pattern.compile(pattern);
            }
        }
        else {
            final String pattern = this.getPatternExpr().getCurrentValue(object);
            if (pattern == null)
                throw new HPersistException("Null string for LIKE pattern");
            this.pattern = Pattern.compile(pattern);
        }

        final String val = this.getExpr().getCurrentValue(object);
        if (val == null)
            throw new HPersistException("Null string for LIKE value");

        final Matcher m = this.getPattern().matcher(val);

        final boolean retval = m.matches();

        return (this.isNot()) ? !retval : retval;
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.expr = new StringLiteral(this.getExpr().getCurrentValue(object));
        else
            retval = false;

        if (this.getPatternExpr().optimizeForConstants(object))
            this.patternExpr = new StringLiteral(this.getPatternExpr().getCurrentValue(object));
        else
            retval = false;

        return retval;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getExpr().getExprVariables();
        retval.addAll(this.getPatternExpr().getExprVariables());
        return retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getExpr().isAConstant() && this.getPatternExpr().isAConstant();
    }

    @Override
    public void setSchema(final ExprSchema schema) {
        this.getExpr().setSchema(schema);
        this.getPatternExpr().setSchema(schema);
    }

}