package org.apache.hadoop.hbase.hbql.client;

@java.lang.annotation.Target({java.lang.annotation.ElementType.TYPE})
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
public @interface HFamily {

    public abstract String name() default "";

    int maxVersions() default -1;
}