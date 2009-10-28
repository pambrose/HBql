package org.apache.yaoql.client;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.io.Serializable;

public abstract class ObjectQueryListenerAdapter<T> implements ObjectQueryListener<T>, Serializable {

    public void onQueryInit() {
    }

    public void onEachObject(T val) throws HBqlException {
    }

    public void onQueryComplete() {
    }
}