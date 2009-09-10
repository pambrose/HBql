import com.imap4j.hbase.hbase.HColumn;
import com.imap4j.hbase.hbase.HFamily;
import com.imap4j.hbase.hbase.HPersistable;
import com.imap4j.hbase.hbase.HTable;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 7:41:00 AM
 */
@HTable(name = "alltypes",
        families = {
                @HFamily(name = "family1", maxVersions = 10),
                @HFamily(name = "family2"),
                @HFamily(name = "family3", maxVersions = 5)
        })
public class AllTypes implements HPersistable {

    @HColumn(key = true)
    private String keyval = null;

    @HColumn(family = "family1")
    private int intValue = -1;

    @HColumn(family = "family1")
    private String stringValue = "";

    public AllTypes() {
    }

    public AllTypes(final String keyval, final int intValue, final String stringValue) {
        this.keyval = keyval;
        this.intValue = intValue;
        this.stringValue = stringValue;
    }
}
