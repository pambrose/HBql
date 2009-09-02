package com.imap4j.hbase.hbql;

import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;
import com.imap4j.hbase.hbql.io.Serialization;
import com.imap4j.hbase.hbql.schema.ClassSchema;
import com.imap4j.hbase.hbql.schema.FieldAttrib;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HUtil {

    public static Scan getScan(final ClassSchema classSchema, final List<String> fieldList, final ExprEvalTree filterExpr) {

        final Scan scan = new Scan();

        for (final String attribName : fieldList) {

            final FieldAttrib attrib = classSchema.getFieldAttribByField(attribName);

            // If it is a map, then request all columns for family
            if (attrib.isMapKeysAsColumns())
                scan.addFamily(attrib.getFamilyName().getBytes());
            else
                scan.addColumn(attrib.getQualifiedName().getBytes());
        }

        if (filterExpr != null) {
            List<String> names = filterExpr.getQualifiedColumnNames();
            scan.setFilter(new PrefixFilter(classSchema, filterExpr));
        }

        return scan;
    }

    final static Serialization ser = Serialization.getSerializationStrategy(Serialization.TYPE.HADOOP);

    public static Serialization getSerialization() {
        return ser;
    }

}
