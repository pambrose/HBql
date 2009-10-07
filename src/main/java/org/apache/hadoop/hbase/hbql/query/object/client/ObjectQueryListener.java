package org.apache.hadoop.hbase.hbql.query.object.client;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public interface ObjectQueryListener<T> {

    void onQueryInit();

    void onEachObject(T val) throws HBqlException;

    void onQueryComplete();
}