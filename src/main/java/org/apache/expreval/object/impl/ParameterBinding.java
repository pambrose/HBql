package org.apache.expreval.object.impl;

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.util.Map;

public abstract class ParameterBinding {

    final Map<String, Object> parameterMap = Maps.newHashMap();

    public abstract String getQuery();

    public void setParameter(final String name, final Object val) {
        this.parameterMap.put(name, val);
    }

    protected void applyParameters(final ExpressionTree exprTree) throws HBqlException {
        for (final String key : this.parameterMap.keySet()) {
            int cnt = exprTree.setParameter(key, this.parameterMap.get(key));
            if (cnt == 0)
                throw new HBqlException("Parameter name " + key + " does not exist in " + this.getQuery());
        }
    }
}
