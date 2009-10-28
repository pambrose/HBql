package org.apache.expreval.util;

import java.util.ArrayList;
import java.util.Arrays;

public class Lists {

    public static <E> ArrayList<E> newArrayList() {
        return new ArrayList<E>();
    }

    public static <E> ArrayList<E> newArrayList(E... vals) {
        return new ArrayList<E>(Arrays.asList(vals));
    }
}
