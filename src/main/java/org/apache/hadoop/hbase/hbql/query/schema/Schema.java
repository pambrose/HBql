package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;
import org.apache.hadoop.hbase.hbql.query.util.Sets;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class Schema implements Serializable {

    private final static int ExprTreeCacheSize = 25;
    List<String> evalList = null;
    Map<String, ExprTree> evalMap = null;

    private final Map<String, ColumnAttrib> columnAttribByVariableNameMap = Maps.newHashMap();
    private final Set<ColumnAttrib> columnAttribSet = Sets.newHashSet();

    public abstract Collection<String> getSchemaFamilyNames(final HConnection connection) throws HBqlException;

    public Set<ColumnAttrib> getColumnAttribSet() {
        return this.columnAttribSet;
    }

    // *** columnAttribByVariableNameMap calls
    private Map<String, ColumnAttrib> getColumnAttribByVariableNameMap() {
        return this.columnAttribByVariableNameMap;
    }

    public boolean containsVariableName(final String varname) {
        return this.getColumnAttribByVariableNameMap().containsKey(varname);
    }

    public ColumnAttrib getAttribByVariableName(final String name) {
        return this.getColumnAttribByVariableNameMap().get(name);
    }

    protected void addAttribToVariableNameMap(final ColumnAttrib attrib,
                                              final String... attribNames) throws HBqlException {

        if (!attrib.isFamilyDefaultAttrib())
            this.getColumnAttribSet().add(attrib);

        for (final String attribName : attribNames) {
            if (this.getColumnAttribByVariableNameMap().containsKey(attribName))
                throw new HBqlException(attribName + " already declared");

            this.getColumnAttribByVariableNameMap().put(attribName, attrib);
        }
    }

    private Map<String, ExprTree> getEvalMap() {

        if (this.evalMap == null) {
            synchronized (this) {
                if (this.evalMap == null) {
                    this.evalMap = Maps.newHashMap();
                    this.evalList = Lists.newArrayList();
                }
            }
        }
        return this.evalMap;
    }

    private List<String> getEvalList() {
        return this.evalList;
    }

    public ExprTree getExprTreeFromCache(final String exprStr) {
        final Map<String, ExprTree> map = this.getEvalMap();
        return map.get(exprStr);
    }

    public synchronized void addToExprTreeCache(final String exprStr, final ExprTree exprTree) {

        final Map<String, ExprTree> map = this.getEvalMap();

        if (!map.containsKey(exprStr)) {

            final List<String> list = this.getEvalList();

            list.add(exprStr);
            map.put(exprStr, exprTree);

            if (list.size() > ExprTreeCacheSize) {
                final String firstOne = list.get(0);
                map.remove(firstOne);
                list.remove(0);
            }
        }
    }
}
