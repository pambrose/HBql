package com.imap4j.hbase.hbql;

import com.imap4j.hbase.antlr.args.KeyRangeArgs;
import com.imap4j.hbase.antlr.args.WhereArgs;
import com.imap4j.hbase.hbql.expr.ExprVariable;
import com.imap4j.hbase.hbql.expr.predicate.ExprEvalTree;
import com.imap4j.hbase.hbql.schema.ClassSchema;
import com.imap4j.hbase.hbql.schema.FieldAttrib;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.HBqlFilter;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 23, 2009
 * Time: 4:49:02 PM
 */
public class HUtil {

    public static List<Scan> getScanList(final ClassSchema classSchema, final List<String> fieldList, final WhereArgs whereExpr) {

        final KeyRangeArgs keys =

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
            scan.setFilter(new HBqlFilter(classSchema, serverFilter));
        }

        return scan;
    }

    public static String getZeroPaddedNumber(final int val, final int width) throws HPersistException {

        final String strval = "" + val;
        final int padsize = width - strval.length();
        if (padsize < 0)
            throw new HPersistException("Value " + val + " exceeded width " + width);

        StringBuilder sbuf = new StringBuilder();
        for (int i = 0; i < padsize; i++)
            sbuf.append("0");

        sbuf.append(strval);
        return sbuf.toString();
    }
}
