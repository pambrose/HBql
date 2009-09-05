package com.imap4j.hbase.hbql;

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
}