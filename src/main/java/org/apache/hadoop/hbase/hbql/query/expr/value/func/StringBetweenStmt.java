package org.apache.hadoop.hbase.hbql.query.expr.value.func;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.node.BooleanValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.query.expr.node.ValueExpr;

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
    public Class<? extends ValueExpr> validateType() throws HPersistException {

        final Class<? extends ValueExpr> type1 = this.getExpr().validateType();
        final Class<? extends ValueExpr> type2 = this.getLower().validateType();
        final Class<? extends ValueExpr> type3 = this.getUpper().validateType();

        if (!ExprTree.isOfType(type1, StringValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in DateBetweenStmt");

        if (!ExprTree.isOfType(type2, StringValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in DateBetweenStmt");

        if (!ExprTree.isOfType(type3, StringValue.class))
            throw new HPersistException("Type " + type1.getName() + " not valid in DateBetweenStmt");

        return BooleanValue.class;
    }

    @Override
    public Boolean getValue(final Object object) throws HPersistException {

        final String strval = this.getExpr().getValue(object);
        final boolean retval = strval.compareTo(this.getLower().getValue(object)) >= 0
                               && strval.compareTo(this.getUpper().getValue(object)) <= 0;

        return (this.isNot()) ? !retval : retval;
    }
}