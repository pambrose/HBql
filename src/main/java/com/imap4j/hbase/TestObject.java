package com.imap4j.hbase;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:39:06 PM
 */
@Table(name = "blogposts")
public class TestObject implements Persistable {

    final String family1 = "post";
    final String family2 = "image";

    final String keyval;

    @Column(family = family1)
    String title = "A title value";

    @Column(family = family1, column = "author")
    String author = "An author value";

    @Column(family = family2)
    String header = "A header value";

    @Column(family = family2, column = "bodyimage")
    String bodyimage = "A bodyimage value";

    public TestObject() {
        this.keyval = "Val-" + System.currentTimeMillis() + "-" + System.nanoTime();
    }

    @Override
    public byte[] getKeyValue() {
        return keyval.getBytes();
    }

    public static void main(String[] args) throws IOException, PersistException {

        Transaction tx = new Transaction();

        int cnt = 100; //10000;
        for (int i = 1; i < cnt; i++) {
            TestObject obj = new TestObject();
            tx.insert(obj);
        }

        tx.commit();

    }
}
