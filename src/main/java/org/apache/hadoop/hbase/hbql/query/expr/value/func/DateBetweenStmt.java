package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.DateValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 25, 2009
 * Time: 6:58:31 PM
 */
public class DateBetweenStmt extends GenericBetweenStmt<DateValue> {

    public DateBetweenStmt(final DateValue expr, final boolean not, final DateValue lower, final DateValue upper) {
        super(not, expr, lower, upper);
    }

    @Override
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr().validateType();
        final Class<? extends ValueExpr> type2 = this.getLower().validateType();
        final Class<? extends ValueExpr> type3 = this.getUpper().validateType();

        if (!ExprTree.isOfType(type1, DateValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in DateBetweenStmt");

        if (!ExprTree.isOfType(type2, DateValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in DateBetweenStmt");

        if (!ExprTree.isOfType(type3, DateValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in DateBetweenStmt");

        return BooleanValue.class;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final long dateval = this.getExpr().getValue(object);
        final boolean retval = dateval >= this.getLower().getValue(object)
                               && dateval <= this.getUpper().getValue(object);

        return (this.isNot()) ? !retval : retval;
    }
}