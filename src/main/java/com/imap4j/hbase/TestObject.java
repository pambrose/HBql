package com.imap4j.hbase;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:39:06 PM
 */
@Table(name = "blogposts")
public class TestObject implements Persistable {

    final String family = "post";

    final String keyval;

    @Column(family = family)
    String title = "A title value";

    @Column(family = family, name = "author")
    String author = "An author value";

    public TestObject() {
        this.keyval = "Val-" + System.currentTimeMillis();
    }

    @Override
    public byte[] getKeyValue() {
        return keyval.getBytes();
    }

}
