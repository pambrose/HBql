package org.apache.hadoop.hbase.hbql.query.object.client;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.Collection;

public interface ObjectQuery<T> {

    String getQuery();

    ObjectResults<T> execute(Collection<T> objs) throws HBqlException;

    void addListener(ObjectQueryListener<T> listener);

    void clearListeners();
}
