package org.apache.hadoop.hbase.contrib.hbql.client;

import java.io.Serializable;

public abstract class QueryListenerAdapter<T> implements QueryListener<T>, Serializable {

    public void onQueryInit() {
    }

    public void onEachRow(T val) {
    }

    public void onQueryComplete() {
    }
}
