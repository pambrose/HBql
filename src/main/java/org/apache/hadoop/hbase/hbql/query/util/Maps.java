package org.apache.hadoop.hbase.hbql.query.util;

import java.util.HashMap;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 11, 2009
 * Time: 1:19:22 PM
 */
public class Maps {
    public static <K, V> HashMap<K, V> newHashMap() {
        return new HashMap<K, V>();
    }
}
