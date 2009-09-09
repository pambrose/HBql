package com.imap4j.hbase.hbql.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.imap4j.hbase.hbql.HPersistException;

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
public class ExprSchema implements Serializable {

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

    protected void setVariableAttribByVariableName(final String name,
                                                   final VariableAttrib variableAttrib) throws HPersistException {
        if (this.getVariableAttribByVariableNameMap().containsKey(name))
            throw new HPersistException(name + " already delcared");
        this.getVariableAttribByVariableNameMap().put(name, variableAttrib);
    }

}
