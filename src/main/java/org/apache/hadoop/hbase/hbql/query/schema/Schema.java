package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.util.Maps;
import org.apache.hadoop.hbase.hbql.query.util.Sets;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 5:30:59 PM
 */
public abstract class Schema implements Serializable {

    private final Map<String, VariableAttrib> variableAttribByVariableNameMap = Maps.newHashMap();
    private final Set<VariableAttrib> variableAttribSet = Sets.newHashSet();

    public Set<VariableAttrib> getAllVariableAttribs() {
        final Set<VariableAttrib> retval = Sets.newHashSet();
        for (final VariableAttrib attrib : this.getVariableAttribSet()) {

            if (attrib.isKeyAttrib())
                continue;

            retval.add(attrib);
        }
        return retval;
    }

    public Set<VariableAttrib> getVariableAttribForFamily(final String familyName) {
        final Set<VariableAttrib> retval = Sets.newHashSet();
        for (final VariableAttrib attrib : this.getVariableAttribSet()) {

            if (attrib.isKeyAttrib())
                continue;

            // Attribs are present twice if an alias is assigned.
            // So add only qualifgied names
            if (!attrib.getColumnName().equals(attrib.getFamilyQualifiedName()))
                continue;

            if (attrib.getFamilyName().length() > 0 && attrib.getFamilyName().equals(familyName))
                retval.add(attrib);
        }
        return retval;
    }

    public Set<VariableAttrib> getVariableAttribSet() {
        return this.variableAttribSet;
    }

    // *** variableAttribByVariableNameMap calls
    private Map<String, VariableAttrib> getVariableAttribByVariableNameMap() {
        return this.variableAttribByVariableNameMap;
    }

    public boolean constainsVariableName(final String varname) {
        return this.getVariableAttribByVariableNameMap().containsKey(varname);
    }

    public VariableAttrib getVariableAttribByVariableName(final String name) {
        return this.getVariableAttribByVariableNameMap().get(name);
    }

    protected void addVariableAttribToVariableNameMap(final VariableAttrib attrib) throws HBqlException {

        final String familyQualifiedName = attrib.getFamilyQualifiedName();

        if (this.getVariableAttribByVariableNameMap().containsKey(familyQualifiedName))
            throw new HBqlException(familyQualifiedName + " already delcared");

        this.getVariableAttribSet().add(attrib);

        this.getVariableAttribByVariableNameMap().put(familyQualifiedName, attrib);

        final String aliasName = attrib.getAliasName();
        if (aliasName != null && !aliasName.equals(familyQualifiedName)) {
            if (this.getVariableAttribByVariableNameMap().containsKey(aliasName))
                throw new HBqlException(aliasName + " already delcared");
            this.getVariableAttribByVariableNameMap().put(aliasName, attrib);
        }
    }
}
