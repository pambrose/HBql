package com.imap4j.hbase.hbql;

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

    private static List<String> packagePath = Lists.newArrayList();

    public static void setPackagePath(final String str) {
        packagePath.clear();
        packagePath.add("");      // Add an entry for the object as defined in command
        packagePath.addAll(Arrays.asList(str.split(":")));
    }

    public static List<String> getPackagePath() {
        return packagePath;
    }
}
