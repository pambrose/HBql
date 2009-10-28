package org.apache.yaoql.client;

import org.apache.expreval.object.impl.ObjectQueryImpl;

public class ObjectQueryManager {

    public static <T> ObjectQueryImpl<T> newObjectQuery(final String query) {
        return new ObjectQueryImpl<T>(query);
    }
}
