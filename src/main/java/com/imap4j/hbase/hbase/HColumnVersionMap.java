package com.imap4j.hbase.hbase;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 5, 2009
 * Time: 7:42:20 AM
 */
@java.lang.annotation.Target({java.lang.annotation.ElementType.FIELD})
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
public @interface HColumnVersionMap {

    String instance() default "";

    String family() default "";

    String column() default "";

    String getter() default "";

    String setter() default "";

    boolean mapKeysAsColumns() default false;
}