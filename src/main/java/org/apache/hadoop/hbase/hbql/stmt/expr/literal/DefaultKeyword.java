package org.apache.hadoop.hbase.hbql.stmt.expr.literal;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 26, 2009
 * Time: 11:15:05 PM
 */
public class DefaultKeyword implements GenericValue {

    public void setExprContext(final ExprContext context) throws HBqlException {

    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return null;
    }

    public GenericValue getOptimizedValue() throws HBqlException {
        return null;
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr, final boolean allowsCollections) throws HBqlException {
        return DefaultKeyword.class;
    }

    public boolean isAConstant() {
        return true;
    }

    public boolean isDefaultKeyword() {
        return true;
    }

    public boolean hasAColumnReference() {
        return false;
    }

    public String asString() {
        return "DEFAULT";
    }

    public void reset() {

    }
}
