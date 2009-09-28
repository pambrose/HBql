package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

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

    public LikeStmt(final GenericValue arg0, final boolean not, final GenericValue arg1) {
        super(arg0, not, arg1);
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

        final String val = (String)this.getArg(0).getValue(object);

        if (val == null)
            throw new HBqlException("Null string for value in " + this.asString());

        final boolean patternConstant = this.getArg(1).isAConstant();

        if (!patternConstant || this.getPattern() == null) {

            final String pvalue = (String)this.getArg(1).getValue(object);

            if (pvalue == null)
                throw new HBqlException("Null string for pattern in " + this.asString());

            this.pattern = Pattern.compile(pvalue);
        }

        final Matcher m = this.getPattern().matcher(val);

        final boolean retval = m.matches();

        return (this.isNot()) ? !retval : retval;
    }

}