package org.apache.hadoop.hbase.hbql.query.object.client;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;

public abstract class ObjectQueryListenerAdapter<T> implements ObjectQueryListener<T>, Serializable {

    public void onQueryInit() {
    }

    public void onEachObject(T val) throws HBqlException {
    }

    public void onQueryComplete() {
    }
}