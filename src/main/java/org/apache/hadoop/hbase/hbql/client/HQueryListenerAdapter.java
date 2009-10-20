package org.apache.hadoop.hbase.hbql.client;

import java.io.Serializable;

public abstract class HQueryListenerAdapter<T> implements HQueryListener<T>, Serializable {

    public void onQueryInit() {
    }

    public void onEachRow(T val) {
    }

    public void onQueryComplete() {
    }
}
