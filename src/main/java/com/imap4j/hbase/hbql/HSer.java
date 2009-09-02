package com.imap4j.hbase.hbql;

import com.imap4j.hbase.hbql.io.Serialization;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HSer {

    final static Serialization ser = Serialization.getSerializationStrategy(Serialization.TYPE.HADOOP);

    public static Serialization getSer() {
        return ser;
    }
}