package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.Serializable;
import java.util.List;
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

    public List<String> getAllNamesList() {
        final List<String> retval = Lists.newArrayList();
        for (final String name : this.getVariableAttribNames()) {

            VariableAttrib attrib = this.getVariableAttribByVariableName(name);

            if (attrib.isKeyAttrib())
                continue;

            retval.add(attrib.getFamilyQualifiedName());
            if (!attrib.getVariableName().equals(attrib.getFamilyQualifiedName()))
                retval.add(attrib.getVariableName());
        }
        return retval;
    }

    public List<String> getFamilyQualifiedNameList() {
        final List<String> retval = Lists.newArrayList();
        for (final String name : this.getVariableAttribNames()) {

            VariableAttrib attrib = this.getVariableAttribByVariableName(name);

            if (attrib.isKeyAttrib())
                continue;

            // Attribs are present twice if an alias is assigned.
            // So add only qualifgied names
            if (!name.equals(attrib.getFamilyQualifiedName()))
                continue;

            retval.add(attrib.getFamilyQualifiedName());
        }
        return retval;
    }

    public List<String> getFieldList(final String familyName) {
        final List<String> retval = Lists.newArrayList();
        for (final String name : this.getVariableAttribNames()) {

            VariableAttrib attrib = this.getVariableAttribByVariableName(name);

            if (attrib.isKeyAttrib())
                continue;

            // Attribs are present twice if an alias is assigned.
            // So add only qualifgied names
            if (!name.equals(attrib.getFamilyQualifiedName()))
                continue;

            if (attrib.getFamilyName().length() > 0 && attrib.getFamilyName().equals(familyName))
                retval.add(attrib.getFamilyQualifiedName());
        }
        return retval;
    }

    public List<String> getAliasAndQualifiedNameCurrentValueList(final List<String> fieldList) {

        final List<String> allVersionList = Lists.newArrayList();
        for (final String field : fieldList) {

            final VariableAttrib attrib = this.getVariableAttribByVariableName(field);

            // Annotated version values will be null
            if (attrib == null)
                continue;

            final String qualifiedName = attrib.getFamilyQualifiedName();
            final String variableName = attrib.getVariableName();

            allVersionList.add(qualifiedName);

            if (!qualifiedName.equals(variableName))
                allVersionList.add(variableName);
        }
        return allVersionList;
    }


    // *** variableAttribByVariableNameMap calls
    private Map<String, VariableAttrib> getVariableAttribByVariableNameMap() {
        return this.variableAttribByVariableNameMap;
    }

    public boolean constainsVariableName(final String varname) {
        return this.getVariableAttribByVariableNameMap().containsKey(varname);
    }

    public Set<String> getVariableAttribNames() {
        return this.getVariableAttribByVariableNameMap().keySet();
    }

    public VariableAttrib getVariableAttribByVariableName(final String name) {
        return this.getVariableAttribByVariableNameMap().get(name);
    }

    protected void addVariableAttribToVariableNameMap(final VariableAttrib attrib) throws HBqlException {

        final String variableName = attrib.getVariableName();

        if (this.getVariableAttribByVariableNameMap().containsKey(variableName))
            throw new HBqlException("In " + this + " " + variableName + " already delcared");

        this.getVariableAttribByVariableNameMap().put(variableName, attrib);

        // If it is an HBase attrib, then add the variable name and the family qualified name
        if (attrib.isHBaseAttrib()) {

            final String familyQualifiedName = ((ColumnAttrib)attrib).getFamilyQualifiedName();

            if (!familyQualifiedName.equals(variableName))
                this.getVariableAttribByVariableNameMap().put(familyQualifiedName, attrib);
        }
    }

}
