package org.apache.hadoop.hbase.hbql.client;

import java.util.Iterator;

public interface HResults<T> extends Iterable<T> {

    void close();

    Iterator<T> iterator();
}
