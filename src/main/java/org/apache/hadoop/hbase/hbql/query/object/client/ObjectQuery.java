package org.apache.hadoop.hbase.hbql.query.object.client;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.Collection;
import java.util.List;

public interface ObjectQuery<T> {

    String getQuery();

    void addListener(ObjectQueryListener<T> listener);

    void clearListeners();

    void setParameter(String name, Object val);

    ObjectResults<T> getResults(Collection<T> objs) throws HBqlException;

    List<T> getResultList(final Collection<T> objs) throws HBqlException;
}
