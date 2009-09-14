package com.imap4j.hbase.hbase;

import com.imap4j.hbase.hbql.schema.VarDescAttrib;
import com.imap4j.hbase.util.Maps;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 12, 2009
 * Time: 11:06:07 PM
 */
public class HRecord {

    final Map<String, VarDescAttrib> types = Maps.newHashMap();
    final Map<String, Object> currentValues = Maps.newHashMap();
    final Map<String, Object> versionValues = Maps.newHashMap();

    public Object getCurrentValue(final String name) {
        return this.currentValues.get(name);
    }

    public Object setCurrentValue(final String name, final Object val) {
        return this.currentValues.put(name, val);
    }

    public Object getVersionedValue(final String name) {
        return this.versionValues.get(name);
    }

    public Object setVersionedValue(final String name, final Object val) {
        return this.versionValues.put(name, val);
    }
}
