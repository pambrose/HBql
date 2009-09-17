package org.apache.hadoop.hbase.hbql.query.util;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 11, 2009
 * Time: 12:17:23 PM
 */
public class Lists {

    public static <E> ArrayList<E> newArrayList() {
        return new ArrayList<E>();
    }

    public static <E> ArrayList<E> newArrayList(E... vals) {
        return new ArrayList<E>(Arrays.asList(vals));
    }

}
