package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.value.literal.StringLiteral;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class StringBetweenStmt extends GenericBetweenStmt<StringValue> {

    public StringBetweenStmt(final StringValue expr, final boolean not, final StringValue lower, final StringValue upper) {
        super(not, expr, lower, upper);
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        boolean retval = true;

        if (this.getExpr().optimizeForConstants(object))
            this.setExpr(new StringLiteral(this.getExpr().getValue(object)));
        else
            retval = false;

        if (this.getLower().optimizeForConstants(object))
            this.setLower(new StringLiteral(this.getLower().getValue(object)));
        else
            retval = false;

        if (this.getUpper().optimizeForConstants(object))
            this.setUpper(new StringLiteral(this.getUpper().getValue(object)));
        else
            retval = false;

        return retval;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final String strval = this.getExpr().getValue(object);
        final boolean retval = strval.compareTo(this.getLower().getValue(object)) >= 0
                               && strval.compareTo(this.getUpper().getValue(object)) <= 0;

        return (this.isNot()) ? !retval : retval;
    }
}