package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.BooleanLiteral;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class LikeStmt extends GenericNotValue {

    private StringValue expr = null;
    private StringValue patternExpr = null;

    private Pattern pattern = null;

    public LikeStmt(final StringValue expr, final boolean not, final StringValue patternExpr) {
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
    public Boolean getValue(final Object object) throws HPersistException {

        if (this.getPatternExpr().isAConstant()) {
            if (this.pattern == null) {
                final String pattern = this.getPatternExpr().getValue(object);
                this.pattern = Pattern.compile(pattern);
            }
        }
        else {
            final String pattern = this.getPatternExpr().getValue(object);
            if (pattern == null)
                throw new HPersistException("Null string for LIKE pattern");
            this.pattern = Pattern.compile(pattern);
        }

        final String val = this.getExpr().getValue(object);
        if (val == null)
            throw new HPersistException("Null string for LIKE value");

        final Matcher m = this.getPattern().matcher(val);

        final boolean retval = m.matches();

        return (this.isNot()) ? !retval : retval;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        this.expr = (StringValue)this.getExpr().getOptimizedValue();
        this.patternExpr = (StringValue)this.getPatternExpr().getOptimizedValue();

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
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
    public void setContext(final ExprTree context) {
        this.getExpr().setContext(context);
        this.getPatternExpr().setContext(context);
    }

}