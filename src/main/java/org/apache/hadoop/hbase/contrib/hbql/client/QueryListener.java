package org.apache.hadoop.hbase.contrib.hbql.client;

public interface QueryListener<T> {

    void onQueryInit();

    void onEachRow(T val);

    void onQueryComplete();
}
