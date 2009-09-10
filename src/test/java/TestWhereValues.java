import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbql.test.WhereValueTests;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class TestWhereValues extends WhereValueTests {


    @Test
    public void keysExpressions() throws HPersistException {
        assertValidInput("WITH KEYS 'aaa' : 'bbb'");
        assertValidInput("WITH KEYS 'sss' : LAST");
        assertValidInput("WITH KEYS 'fff' : 'ggg', 'sss': LAST, 'sssd':LAST");
    }

    @Test
    public void timeExpressions() throws HPersistException {
        assertValidInput("WITH TIME RANGE NOW : NOW");
        assertValidInput("WITH TIME RANGE NOW : TOMORROW");
    }

    @Test
    public void versionExpressions() throws HPersistException {
        assertValidInput("WITH VERSIONS 12");
    }

    @Test
    public void timerangeExpressions() throws HPersistException {
        assertValidInput("WITH TIME RANGE NOW : TOMORROW");
        assertValidInput("WITH TIME RANGE NOW - DAY(1) : TOMORROW + DAY(2)");
        assertValidInput("WITH TIME RANGE DATE('mm/dd/yy', '10/31/94') - DAY(1) : TOMORROW + DAY(2)");
    }

    @Test
    public void clientExpressions() throws HPersistException {
        assertValidInput("WITH CLIENT FILTER TRUE");
        assertValidInput("WITH CLIENT FILTER {col1 as int} col1 < 4");
        assertValidInput("WITH CLIENT FILTER {fam1:col1 as int} fam1:col1 < 4");
    }

}