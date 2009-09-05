package com.imap4j.hbase.hbql;


/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:47:52 PM
 */
@java.lang.annotation.Target({java.lang.annotation.ElementType.FIELD})
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
public @interface HColumn {

    boolean key() default false;

    String family() default "";

    String column() default "";

    String getter() default "";

    String setter() default "";

    boolean mapKeysAsColumns() default false;

}