package org.apache.hadoop.hbase.hbql.stmt.schema;


import org.apache.hadoop.hbase.hbql.stmt.util.Lists;

import java.util.Arrays;
import java.util.List;

public class EnvVars {

    private static List<String> packagePath = Lists.newArrayList();

    public static void setPackagePath(final String str) {
        packagePath.clear();
        packagePath.add("");      // Add an entry for the object as defined in statement
        packagePath.addAll(Arrays.asList(str.split(":")));
    }

    public static List<String> getPackagePath() {
        return packagePath;
    }
}
