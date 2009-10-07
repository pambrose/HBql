package org.apache.hadoop.hbase.hbql.client;

@java.lang.annotation.Target({java.lang.annotation.ElementType.TYPE})
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
public @interface HTable {

    java.lang.String name() default "";

    HFamily[] families();
}