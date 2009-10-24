package org.apache.hadoop.hbase.hbql.query.impl.object;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.stmt.util.Maps;

import java.util.Map;

public abstract class ParameterBinding {

    final Map<String, Object> parameterMap = Maps.newHashMap();

    public abstract String getQuery();

    public void setParameter(final String name, final Object val) {
        this.parameterMap.put(name, val);
    }

    protected void applyParameters(final ExprTree exprTree) throws HBqlException {
        for (final String key : this.parameterMap.keySet()) {
            int cnt = exprTree.setParameter(key, this.parameterMap.get(key));
            if (cnt == 0)
                throw new HBqlException("Parameter name " + key + " does not exist in " + this.getQuery());
        }
    }
}
