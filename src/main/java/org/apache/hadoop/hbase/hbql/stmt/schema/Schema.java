package org.apache.hadoop.hbase.hbql.stmt.schema;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.stmt.antlr.HBql;
import org.apache.hadoop.hbase.hbql.stmt.antlr.HBqlParser;
import org.apache.hadoop.hbase.hbql.stmt.expr.ExpressionTree;
import org.apache.hadoop.hbase.hbql.stmt.util.Lists;
import org.apache.hadoop.hbase.hbql.stmt.util.Maps;
import org.apache.hadoop.hbase.hbql.stmt.util.Sets;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class Schema implements Serializable {

    private final Map<String, ColumnAttrib> columnAttribByVariableNameMap = Maps.newHashMap();
    private final Set<ColumnAttrib> columnAttribSet = Sets.newHashSet();
    private List<String> evalList = null;
    private Map<String, ExpressionTree> evalMap = null;
    private int exprTreeCacheSize = 25;
    private final String schemaName;

    protected Schema(final String schemaName) {
        this.schemaName = schemaName;
    }

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

    public void resetDefaultValues() {
        for (final ColumnAttrib attrib : this.getColumnAttribSet())
            attrib.resetDefaultValue();
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

    private Map<String, ExpressionTree> getEvalMap() {

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

    public String getSchemaName() {
        return this.schemaName;
    }


    private List<String> getEvalList() {
        return this.evalList;
    }

    public String toString() {
        return this.getSchemaName();
    }

    public int getEvalCacheSize() {
        return this.exprTreeCacheSize;
    }

    public void setEvalCacheSize(final int size) {

        if (size > 0) {
            this.exprTreeCacheSize = size;

            // Reset existing cache
            final Map<String, ExpressionTree> map = this.getEvalMap();
            final List<String> list = this.getEvalList();
            map.clear();
            list.clear();
        }
    }

    public ExpressionTree getExprTree(final String str) throws RecognitionException {
        final Map<String, ExpressionTree> map = this.getEvalMap();
        ExpressionTree exprTree = map.get(str);
        if (exprTree == null) {
            final HBqlParser parser = HBql.newParser(str);
            exprTree = parser.nodescWhereExpr();
            exprTree.setSchema(this);
            this.addToExprTreeCache(str, exprTree);
        }
        else {
            exprTree.reset();
        }
        return exprTree;
    }

    private synchronized void addToExprTreeCache(final String exprStr, final ExpressionTree exprTree) {

        final Map<String, ExpressionTree> map = this.getEvalMap();

        if (!map.containsKey(exprStr)) {

            final List<String> list = this.getEvalList();

            list.add(exprStr);
            map.put(exprStr, exprTree);

            if (list.size() > exprTreeCacheSize) {
                final String firstOne = list.get(0);
                map.remove(firstOne);
                list.remove(0);
            }
        }
    }
}
