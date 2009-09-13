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
    final Map<String, Object> values = Maps.newHashMap();

    public Object getValue(final String name) {
        return this.values.get(name);
    }
}
