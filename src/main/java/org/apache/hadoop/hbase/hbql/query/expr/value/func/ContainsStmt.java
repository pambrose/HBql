package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class ContainsStmt extends GenericStringPatternStmt {

    public ContainsStmt(final GenericValue valueExpr, final boolean not, final GenericValue patternExpr) {
        super(valueExpr, not, patternExpr);
    }

    @Override
    protected String getFunctionName() {
        return "CONTAINS";
    }

    @Override
    public Boolean getValue(final Object object) throws HBqlException {

        final String val1 = (String)this.getArg(0).getValue(object);
        final String val2 = (String)this.getArg(1).getValue(object);

        if (val1 == null)
            throw new HBqlException("Null string for value in " + this.asString());

        if (val2 == null)
            throw new HBqlException("Null string for pattern in " + this.asString());

        final boolean retval = val1.contains(val2);

        return (this.isNot()) ? !retval : retval;
    }
}