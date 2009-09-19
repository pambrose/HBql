package org.apache.hadoop.hbase.hbql.query.schema;

import org.apache.hadoop.hbase.hbql.client.HPersistException;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

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
public abstract class Schema implements Serializable {

    private final Map<String, VariableAttrib> variableAttribByVariableNameMap = Maps.newHashMap();

    public List<String> getFieldList() {
        final List<String> retval = Lists.newArrayList();
        for (final VariableAttrib attrib : this.getVariableAttribs()) {
            if (attrib.isKeyAttrib())
                continue;
            retval.add(attrib.getVariableName());
        }
        return retval;
    }

    public List<String> getAliasAndQualifiedNameFieldList(final List<String> fieldList) {

        final List<String> allVersionList = Lists.newArrayList();
        for (final String field : fieldList) {

            final VariableAttrib attrib = this.getVariableAttribByVariableName(field);

            // Annotated version values will be null
            if (attrib == null)
                continue;

            final String qualifiedname = attrib.getFamilyQualifiedName();
            final String variableName = attrib.getVariableName();
            if (qualifiedname.equals(variableName)) {
                allVersionList.add(qualifiedname);
            }
            else {
                allVersionList.add(qualifiedname);
                allVersionList.add(variableName);
            }

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

    public Collection<VariableAttrib> getVariableAttribs() {
        return this.getVariableAttribByVariableNameMap().values();
    }

    public VariableAttrib getVariableAttribByVariableName(final String name) {
        return this.getVariableAttribByVariableNameMap().get(name);
    }

    protected void addVariableAttribToVariableNameMap(final VariableAttrib attrib) throws HPersistException {

        final String variableName = attrib.getVariableName();
        if (this.getVariableAttribByVariableNameMap().containsKey(variableName))
            throw new HPersistException("In " + this + " " + variableName + " already delcared");
        this.getVariableAttribByVariableNameMap().put(variableName, attrib);

        // If it is an HBase attrib, then add the variable name and the family qualified name
        if (attrib.isHBaseAttrib()) {
            final String familyQualifiedName = ((ColumnAttrib)attrib).getFamilyQualifiedName();
            if (!familyQualifiedName.equals(variableName))
                this.getVariableAttribByVariableNameMap().put(familyQualifiedName, attrib);
        }
    }

    // This is used for tests
    public static Schema getSchema(final Object recordObj) throws HPersistException {

        if (recordObj == null)
            return null;

        try {
            return AnnotationSchema.getAnnotationSchema(recordObj);
        }
        catch (HPersistException e) {
            // Not annotated properly
        }

        return ReflectionSchema.getReflectionSchema(recordObj);
    }
}
