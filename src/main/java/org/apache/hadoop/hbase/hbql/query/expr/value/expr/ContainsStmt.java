package org.apache.hadoop.hbase.hbql.query.expr.value.expr;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class ContainsStmt extends GenericStringPatternStmt {

    public ContainsStmt(final GenericValue valueExpr, final boolean not, final GenericValue patternExpr) {
        super(valueExpr, not, patternExpr);
    }

    protected String getFunctionName() {
        return "CONTAINS";
    }

    public Boolean getValue(final Object object) throws HBqlException, ResultMissingColumnException {

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