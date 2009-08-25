package com.imap4j.hbase.hql;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 21, 2009
 * Time: 12:51:41 PM
 */
public class EnvVars {

    private static List<String> classpath = Lists.newArrayList();

    public static void setClasspath(final String str) {
        classpath.clear();
        classpath.add("");      // Add an entry for the object as defined in command
        classpath.addAll(Arrays.asList(str.split(":")));
    }

    public static List<String> getClasspath() {
        return classpath;
    }
}
