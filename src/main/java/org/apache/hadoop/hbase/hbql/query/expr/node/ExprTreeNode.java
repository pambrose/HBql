package org.apache.hadoop.hbase.hbql.query.expr.node;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 11:52:39 AM
 */
public interface ExprTreeNode extends Serializable {

    ValueExpr getOptimizedValue(final Object object) throws HPersistException;

    List<ExprVariable> getExprVariables();

    boolean isAConstant();

    void setContext(ExprTree context);

}
