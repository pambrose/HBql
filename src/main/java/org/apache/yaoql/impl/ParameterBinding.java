package org.apache.yaoql.impl;

import org.apache.expreval.client.HBqlException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.util.Maps;

import java.util.Map;

public abstract class ParameterBinding {

    final Map<String, Object> parameterMap = Maps.newHashMap();

    public abstract String getQuery();

    public void setParameter(final String name, final Object val) {
        this.getParameterMap().put(name, val);
    }

    protected void applyParameters(final ExpressionTree exprTree) throws HBqlException {
        for (final String key : this.getParameterMap().keySet()) {
            int cnt = exprTree.setParameter(key, this.getParameterMap().get(key));
            if (cnt == 0)
                throw new HBqlException("Parameter name " + key + " does not exist in " + this.getQuery());
        }
    }

    private Map<String, Object> getParameterMap() {
        return parameterMap;
    }
}
