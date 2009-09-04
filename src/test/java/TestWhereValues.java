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
    public void whereExpressions() throws HPersistException {
        assertValidInput("", "KEYS 'aaa' : 'bbb'");
    }

}