package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 15, 2009
 * Time: 8:58:43 PM
 */
public class ElementMap<T> {

    private final Map<String, T> map = Maps.newHashMap();

    private final HRecordImpl hrecord;

    public ElementMap(final HRecordImpl hrecord) {
        this.hrecord = hrecord;
    }

    private HRecordImpl getHRecord() {
        return this.hrecord;
    }

    public Map<String, T> getMap() {
        return map;
    }

    private String getNameToUse(final String name) {
        final ColumnAttrib attrib = this.getHRecord().getSchema().getAttribByVariableName(name);
        if (attrib == null)
            return name;
        else
            return attrib.getFamilyQualifiedName();
    }

    public void addElement(final String name, final T value) {
        this.getMap().put(this.getNameToUse(name), value);
    }

    public boolean containsName(final String name) {
        return this.getMap().containsKey(name);
    }

    private T getElement(final String name) {
        return this.getMap().get(name);
    }

    public T findElement(final String name) {

        // First try the name given.
        // If that doesn't work, then try qualified name
        if (this.containsName(name))
            return this.getElement(name);

        // Look up by  alias name
        final ColumnAttrib attrib = this.getHRecord().getSchema().getAttribByVariableName(name);

        if (attrib != null) {
            final String qualifiedName = attrib.getFamilyQualifiedName();
            if (!qualifiedName.equals(name) && this.containsName(qualifiedName))
                return this.getElement(qualifiedName);
        }

        return null;
    }

    public void clear() {

        this.getMap().clear();
    }
}
