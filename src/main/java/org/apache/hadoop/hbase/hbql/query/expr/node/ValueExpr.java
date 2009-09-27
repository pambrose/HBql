package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 3, 2009
 * Time: 8:13:01 PM
 */
public interface ValueExpr extends Serializable {

    Object getValue(final Object object) throws HBqlException;

    ValueExpr getOptimizedValue() throws HBqlException;

    Class<? extends ValueExpr> validateTypes(final ValueExpr parentExpr,
                                             final boolean allowsCollections) throws TypeException;

    boolean isAConstant() throws HBqlException;

    void setContext(ExprTree context);

    String asString();
}
