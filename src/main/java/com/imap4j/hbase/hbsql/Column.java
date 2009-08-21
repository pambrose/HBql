package com.imap4j.hbase.hbsql;


/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:47:52 PM
 */
@java.lang.annotation.Target({java.lang.annotation.ElementType.METHOD, java.lang.annotation.ElementType.FIELD})
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
public @interface Column {

    String family() default "";

    String column() default "";

    String lookup() default "";

    boolean mapKeysAsColumns() default false;

}