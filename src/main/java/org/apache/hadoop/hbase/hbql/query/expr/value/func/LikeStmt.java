package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
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

    private ValueExpr valueExpr = null;
    private ValueExpr patternExpr = null;

    private Pattern pattern = null;

    public LikeStmt(final ValueExpr valueExpr, final boolean not, final ValueExpr patternExpr) {
        super(not);
        this.valueExpr = valueExpr;
        this.patternExpr = patternExpr;
    }

    private ValueExpr getValueExpr() {
        return this.valueExpr;
    }

    private ValueExpr getPatternExpr() {
        return this.patternExpr;
    }

    private Pattern getPattern() {
        return this.pattern;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        if (this.getPatternExpr().isAConstant()) {
            if (this.pattern == null) {
                final String pattern = (String)this.getPatternExpr().getValue(object);
                this.pattern = Pattern.compile(pattern);
            }
        }
        else {
            final String pattern = (String)this.getPatternExpr().getValue(object);
            if (pattern == null)
                throw new HPersistException("Null string for LIKE pattern");
            this.pattern = Pattern.compile(pattern);
        }

        final String val = (String)this.getValueExpr().getValue(object);
        if (val == null)
            throw new HPersistException("Null string for LIKE value");

        final Matcher m = this.getPattern().matcher(val);

        final boolean retval = m.matches();

        return (this.isNot()) ? !retval : retval;
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getValueExpr().validateType();
        final Class<? extends ValueExpr> type2 = this.getPatternExpr().validateType();

        if (!type1.equals(type2))
            throw new HPersistException("Type mismatch in LikeStmt");

        if (!type1.equals(StringValue.class))
            throw new HPersistException("Invalid type " + type1.getName() + " in LikeStmt");

        return BooleanValue.class;
    }

    @Override
    public ValueExpr getOptimizedValue() throws HPersistException {
        this.valueExpr = this.getValueExpr().getOptimizedValue();
        this.patternExpr = this.getPatternExpr().getOptimizedValue();

        return this.isAConstant() ? new BooleanLiteral(this.getValue(null)) : this;
    }

    @Override
    public List<ExprVariable> getExprVariables() {
        final List<ExprVariable> retval = this.getValueExpr().getExprVariables();
        retval.addAll(this.getPatternExpr().getExprVariables());
        return retval;
    }

    @Override
    public boolean isAConstant() {
        return this.getValueExpr().isAConstant() && this.getPatternExpr().isAConstant();
    }

    @Override
    public void setContext(final ExprTree context) {
        this.getValueExpr().setContext(context);
        this.getPatternExpr().setContext(context);
    }

    @Override
    public void setParam(final String param, final Object val) {
        this.getValueExpr().setParam(param, val);
        this.getPatternExpr().setParam(param, val);
    }
}