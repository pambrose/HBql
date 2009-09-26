package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class LikeStmt extends GenericStringPatternStmt {

    private Pattern pattern = null;

    public LikeStmt(final ValueExpr valueExpr, final boolean not, final ValueExpr patternExpr) {
        super(valueExpr, not, patternExpr);
    }

    private Pattern getPattern() {
        return this.pattern;
    }

    @Override
    protected String getFunctionName() {
        return "LIKE";
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        if (this.getPatternExpr().isAConstant()) {
            if (this.getPattern() == null) {
                final String pattern = (String)this.getPatternExpr().getValue(object);
                this.pattern = Pattern.compile(pattern);
            }
        }
        else {
            final String pvalue = (String)this.getPatternExpr().getValue(object);
            if (pvalue == null)
                throw new HBqlException("Null string for pattern in " + this.asString());
            this.pattern = Pattern.compile(pvalue);
        }

        final String val = (String)this.getValueExpr().getValue(object);
        if (val == null)
            throw new HBqlException("Null string for value in " + this.asString());

        final Matcher m = this.getPattern().matcher(val);

        final boolean retval = m.matches();

        return (this.isNot()) ? !retval : retval;
    }

}