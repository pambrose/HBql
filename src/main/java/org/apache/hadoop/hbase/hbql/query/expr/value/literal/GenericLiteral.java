package org.apache.hadoop.hbase.hbql.query.expr.value.literal;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.expr.ExprVariable;
import org.apache.hadoop.hbase.hbql.query.expr.node.ExprTreeNode;
import org.apache.hadoop.hbase.hbql.query.schema.Schema;
import org.apache.hadoop.hbase.hbql.query.util.Lists;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 31, 2009
 * Time: 12:27:29 PM
 */
public abstract class GenericLiteral implements ExprTreeNode {

    @Override
    public List<ExprVariable> getExprVariables() {
        return Lists.newArrayList();
    }

    @Override
    public boolean optimizeForConstants(final Object object) throws HPersistException {
        return true;
    }

    @Override
    public boolean isAConstant() {
        return true;
    }

    @Override
    public void setSchema(final Schema schema) {
    }
}
