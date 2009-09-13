package com.imap4j.hbase.hbql.schema;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.util.Lists;
import com.imap4j.hbase.util.Maps;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 5:30:59 PM
 */
public abstract class ExprSchema implements Serializable {

    private final Map<String, VariableAttrib> variableAttribByVariableNameMap = Maps.newHashMap();

    public List<String> getFieldList() {
        final List<String> retval = Lists.newArrayList();
        for (final VariableAttrib attrib : this.getVariableAttribs()) {
            if (attrib.isKey())
                continue;
            retval.add(attrib.getVariableName());
        }
        return retval;
    }

    public abstract String getSchemaName();

    public abstract String getTableName();

    public Object newInstance() throws IllegalAccessException, InstantiationException {
        return null;
    }

    // *** variableAttribByVariableNameMap calls
    private Map<String, VariableAttrib> getVariableAttribByVariableNameMap() {
        return this.variableAttribByVariableNameMap;
    }

    public boolean constainsVariableName(final String varname) {
        return this.getVariableAttribByVariableNameMap().containsKey(varname);
    }

    public Collection<VariableAttrib> getVariableAttribs() {
        return this.getVariableAttribByVariableNameMap().values();
    }

    public VariableAttrib getVariableAttribByVariableName(final String name) {
        return this.getVariableAttribByVariableNameMap().get(name);
    }

    protected void addVariableAttrib(final VariableAttrib variableAttrib) throws HPersistException {
        final String name = variableAttrib.getVariableName();
        if (this.getVariableAttribByVariableNameMap().containsKey(name))
            throw new HPersistException(name + " already delcared");
        this.getVariableAttribByVariableNameMap().put(name, variableAttrib);
    }
}
