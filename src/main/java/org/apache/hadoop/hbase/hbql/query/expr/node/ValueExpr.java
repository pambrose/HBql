package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 3, 2009
 * Time: 8:13:01 PM
 */
public interface ValueExpr extends Serializable {

    Object getValue(final Object object) throws HPersistException;

    ValueExpr getOptimizedValue() throws HPersistException;

    List<ExprVariable> getExprVariables();

    boolean isAConstant();

    void setContext(ExprTree context);

}
