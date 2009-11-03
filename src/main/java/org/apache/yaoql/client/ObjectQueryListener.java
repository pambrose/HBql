package org.apache.yaoql.client;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public interface ObjectQueryListener<T> {

    void onQueryInit();

    void onEachObject(T val) throws HBqlException;

    void onQueryComplete();
}