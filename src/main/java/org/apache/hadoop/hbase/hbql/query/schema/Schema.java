package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.query.expr.ExprContext;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;
import org.apache.hadoop.hbase.hbql.query.util.Sets;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class Schema implements Serializable {

    List<ExprContext> evalList = null;

    private final Map<String, ColumnAttrib> columnAttribByVariableNameMap = Maps.newHashMap();
    private final Set<ColumnAttrib> columnAttribSet = Sets.newHashSet();

    public abstract Collection<String> getSchemaFamilyNames(final HConnection connection) throws HBqlException;

    public List<ColumnAttrib> getAllAttribs() {
        final List<ColumnAttrib> retval = Lists.newArrayList();
        for (final ColumnAttrib attrib : this.getColumnAttribSet()) {

            if (attrib.isKeyAttrib())
                continue;

            retval.add(attrib);
        }
        return retval;
    }

    public List<ColumnAttrib> getAttribForFamily(final String familyName) {
        final List<ColumnAttrib> retval = Lists.newArrayList();
        for (final ColumnAttrib attrib : this.getColumnAttribSet()) {

            if (attrib.isKeyAttrib())
                continue;

            if (attrib.getFamilyName().length() > 0 && attrib.getFamilyName().equals(familyName))
                retval.add(attrib);
        }
        return retval;
    }

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

        this.getColumnAttribSet().add(attrib);

        for (final String attribName : attribNames) {
            if (this.getColumnAttribByVariableNameMap().containsKey(attribName))
                throw new HBqlException(attribName + " already declared");

            this.getColumnAttribByVariableNameMap().put(attribName, attrib);
        }
    }

    private List<ExprContext> getEvalList() {

        if (this.evalList == null) {
            synchronized (this) {
                if (this.evalList == null)
                    this.evalList = Lists.newArrayList();
            }
        }

        return this.evalList;
    }

    public ExprContext getExprTree(final String exprStr) {

        final List<ExprContext> list = this.getEvalList();
        final int pos = list.indexOf(exprStr);
        if (pos >= 0)
            return list.get(pos);
        else
            return null;
    }

    public synchronized void addExprTree(final ExprContext exprTree) {
        final List<ExprContext> list = this.getEvalList();
        list.add(exprTree);
        if (list.size() > 25)
            list.remove(0);
    }
}
