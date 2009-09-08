import com.imap4j.hbase.hbql.HPersistException;
import com.imap4j.hbase.hbql.test.WhereExprTests;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 27, 2009
 * Time: 2:13:47 PM
 */
public class TestIntervalValues extends WhereExprTests {


    @Test
    public void keysExpressions() throws HPersistException {
        assertEvalTrue("NOW < NOW+YEAR(1)");
        assertEvalTrue("NOW == NOW+YEAR(1)-YEAR(1)");
        assertEvalTrue("NOW+YEAR(2) > NOW+YEAR(1)");
    }

}