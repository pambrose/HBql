package org.apache.hadoop.hbase.hbql.client;

import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 6, 2009
 * Time: 12:22:10 PM
 */
public interface HResults<T> extends Iterable<T> {

    void close();

    @Override
    Iterator<T> iterator();
}
