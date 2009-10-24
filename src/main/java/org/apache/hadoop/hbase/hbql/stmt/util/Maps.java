package org.apache.hadoop.hbase.hbql.stmt.util;

import java.util.HashMap;

public class Maps {
    public static <K, V> HashMap<K, V> newHashMap() {
        return new HashMap<K, V>();
    }
}
