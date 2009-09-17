import com.imap4j.hbase.hbase.HPersistException;
import org.junit.Test;
import util.WhereValueTests;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class WithValuesTest extends WhereValueTests {


    @Test
    public void keysExpressions() throws HPersistException {
        assertValidInput("WITH KEYS 'aaa' TO 'bbb'");
        assertValidInput("WITH KEYS 'sss' TO LAST");
        assertValidInput("WITH KEYS 'fff' TO 'ggg', 'sss'TO LAST, 'sssd' TO LAST");
    }

    @Test
    public void timeExpressions() throws HPersistException {
        assertValidInput("WITH TIME RANGE NOW() TO NOW()");
        assertValidInput("WITH TIME RANGE NOW() TO NOW()+DAY(1)");
    }

    @Test
    public void versionExpressions() throws HPersistException {
        assertValidInput("WITH VERSIONS 12");
    }

    @Test
    public void timerangeExpressions() throws HPersistException {
        assertValidInput("WITH TIME RANGE NOW() TO NOW()+DAY(1)");
        assertValidInput("WITH TIME RANGE NOW() - DAY(1) TO NOW() + DAY(1) + DAY(2)");
        assertValidInput("WITH TIME RANGE DATE('mm/dd/yy', '10/31/94') - DAY(1) TO NOW()+DAY(1) + DAY(2)");
    }

    @Test
    public void clientFilterExpressions() throws HPersistException {
        assertValidInput("WITH CLIENT FILTER WHERE TRUE");
        assertValidInput("WITH CLIENT FILTER WHERE {col1 int} col1 < 4");
        assertValidInput("WITH CLIENT FILTER WHERE {fam1:col1 int} fam1:col1 < 4");
    }

    @Test
    public void serverFilterExpressions() throws HPersistException {
        assertValidInput("WITH SERVER FILTER WHERE TRUE");
        assertValidInput("WITH SERVER FILTER WHERE {col1 int} col1 < 4");
        assertValidInput("WITH SERVER FILTER WHERE {fam1:col1 int alias d} fam1:col1 < 4 OR d > 3");
    }

}