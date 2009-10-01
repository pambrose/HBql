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

    private final Map<String, ColumnAttrib> columnAttribByVariableNameMap = Maps.newHashMap();
    private final Set<ColumnAttrib> columnAttribSet = Sets.newHashSet();

    public Set<ColumnAttrib> getAllAttribs() {
        final Set<ColumnAttrib> retval = Sets.newHashSet();
        for (final ColumnAttrib attrib : this.getColumnAttribSet()) {

            if (attrib.isKeyAttrib())
                continue;

            retval.add(attrib);
        }
        return retval;
    }

    public Set<ColumnAttrib> getAttribForFamily(final String familyName) {
        final Set<ColumnAttrib> retval = Sets.newHashSet();
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

    public boolean constainsVariableName(final String varname) {
        return this.getColumnAttribByVariableNameMap().containsKey(varname);
    }

    public ColumnAttrib getAttribByVariableName(final String name) {
        return this.getColumnAttribByVariableNameMap().get(name);
    }

    protected void addAttribToVariableNameMap(final ColumnAttrib attrib) throws HBqlException {

        final String familyQualifiedName = attrib.getFamilyQualifiedName();

        if (this.getColumnAttribByVariableNameMap().containsKey(familyQualifiedName))
            throw new HBqlException(familyQualifiedName + " already delcared");

        this.getColumnAttribSet().add(attrib);

        this.getColumnAttribByVariableNameMap().put(familyQualifiedName, attrib);

        final String aliasName = attrib.getAliasName();
        if (aliasName != null && !aliasName.equals(familyQualifiedName)) {
            if (this.getColumnAttribByVariableNameMap().containsKey(aliasName))
                throw new HBqlException(aliasName + " already delcared");
            this.getColumnAttribByVariableNameMap().put(aliasName, attrib);
        }
    }
}
