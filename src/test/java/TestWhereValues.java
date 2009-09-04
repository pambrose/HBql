import com.imap4j.hbase.hbql.HPersistException;
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
        assertValidInput("KEYS 'aaa' : 'bbb'");
        assertValidInput("KEYS 'sss'");
        assertValidInput("KEYS 'fff' : 'ggg', 'sss', 'sssd'");
    }

    @Test
    public void timeExpressions() throws HPersistException {
        assertValidInput("TIME RANGE NOW : NOW");
        assertValidInput("TIME RANGE NOW : TOMORROW");
    }

    @Test
    public void versionExpressions() throws HPersistException {
        assertValidInput("VERSIONS 12");
    }

}