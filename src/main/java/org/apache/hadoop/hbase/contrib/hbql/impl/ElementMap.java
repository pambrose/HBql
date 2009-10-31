package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;

import java.io.Serializable;
import java.util.Map;

public class ElementMap<T extends Serializable> implements Serializable {

    private final Map<String, T> map = Maps.newHashMap();

    private final RecordImpl record;

    public ElementMap(final RecordImpl record) {
        this.record = record;
    }

    private RecordImpl getRecord() {
        return this.record;
    }

    public Map<String, T> getMap() {
        return map;
    }

    private String getNameToUse(final String name) {
        final ColumnAttrib attrib = this.getRecord().getSchema().getAttribByVariableName(name);
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
        final ColumnAttrib attrib = this.getRecord().getSchema().getAttribByVariableName(name);

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
