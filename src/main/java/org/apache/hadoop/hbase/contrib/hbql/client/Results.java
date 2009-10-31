package org.apache.hadoop.hbase.contrib.hbql.client;

import java.util.Iterator;

public interface Results<T> extends Iterable<T> {

    void close();

    Iterator<T> iterator();
}
