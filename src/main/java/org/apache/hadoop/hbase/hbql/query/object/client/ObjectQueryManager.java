package org.apache.hadoop.hbase.hbql.query.object.client;

import org.apache.hadoop.hbase.hbql.query.impl.object.ObjectQueryImpl;

public class ObjectQueryManager {

    public static <T> ObjectQueryImpl<T> newObjectQuery(final String query) {
        return new ObjectQueryImpl<T>(query);
    }
}
