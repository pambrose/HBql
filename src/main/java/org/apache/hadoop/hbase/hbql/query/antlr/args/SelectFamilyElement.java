package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.FamilyAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Oct 1, 2009
 * Time: 8:40:58 PM
 */
public class SelectFamilyElement implements SelectElement {

    private final String familyName;
    private final boolean useAllFamilies;

    public SelectFamilyElement(final String familyName) {
        if (familyName != null && familyName.equals("*")) {
            this.useAllFamilies = true;
            this.familyName = "*";
        }
        else {
            this.useAllFamilies = false;
            this.familyName = (familyName == null) ? null : familyName.replace(" ", "").replace(":*", "");
        }
    }

    public static SelectElement newAllFamilies() {
        return new SelectFamilyElement("*");
    }

    public static SelectFamilyElement newFamilyElement(final String family) {
        return new SelectFamilyElement(family);
    }

    public String getFamilyName() {
        return this.familyName;
    }

    @Override
    public void processSelectElement(final HBaseSchema schema,
                                     final List<ColumnAttrib> selectAttribList) throws HBqlException {
        if (this.useAllFamilies) {
            for (final String familyName : schema.getFamilySet())
                selectAttribList.add(new FamilyAttrib(familyName));
        }
        else {
            if (!schema.containsFamilyNameInFamilyNameMap(this.getFamilyName()))
                throw new HBqlException("Invalid family name: " + this.getFamilyName());

            selectAttribList.add(new FamilyAttrib(this.getFamilyName()));
        }
    }

}
