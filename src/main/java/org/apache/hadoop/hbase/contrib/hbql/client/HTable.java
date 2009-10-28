package org.apache.hadoop.hbase.contrib.hbql.client;

@java.lang.annotation.Target({java.lang.annotation.ElementType.TYPE})
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)

// START SNIPPET: doc
public @interface HTable {

    java.lang.String name() default "";

    HFamily[] families();
}
// END SNIPPET: doc
