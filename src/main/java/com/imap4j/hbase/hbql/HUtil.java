package com.imap4j.hbase.hbql;

import com.imap4j.hbase.antlr.args.WhereArgs;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;
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

    public static Scan getScan(final ClassSchema classSchema, final List<String> fieldList, final WhereArgs whereExpr) {

        final Scan scan = new Scan();

        for (final String attribName : fieldList) {

            final FieldAttrib attrib = classSchema.getFieldAttribByName(attribName);

            // If it is a map, then request all columns for family
            if (attrib.isMapKeysAsColumns())
                scan.addFamily(attrib.getFamilyName().getBytes());
            else
                scan.addColumn(attrib.getQualifiedName().getBytes());
        }

        final ExprEvalTree serverFilter = whereExpr.getServerFilterArgs();

        if (serverFilter != null) {
            List<ExprVariable> names = serverFilter.getExprVariables();
            scan.setFilter(new PrefixFilter(classSchema, serverFilter));
        }

        return scan;
    }
}
