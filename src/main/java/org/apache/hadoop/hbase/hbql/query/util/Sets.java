package org.apache.hadoop.hbase.hbql.query.util;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 11, 2009
 * Time: 12:17:23 PM
 */
public class Sets {

    public static <E> HashSet<E> newHashSet() {
        return new HashSet<E>();
    }

    public static <E> HashSet<E> newHashSet(E... vals) {
        return new HashSet<E>(Arrays.asList(vals));
    }

}