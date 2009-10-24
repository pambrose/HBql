// START SNIPPET: doc
package org.apache.hadoop.hbase.hbql.client;

@java.lang.annotation.Target({java.lang.annotation.ElementType.FIELD})
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)

public @interface HColumnVersionMap {

    String instance() default "";

    String family() default "";

    String column() default "";

    String getter() default "";

    String setter() default "";

    boolean mapKeysAsColumns() default false;

    boolean familyDefault() default false;
}
// END SNIPPET: doc
