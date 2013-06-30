package org.elasticsearch.river.subversion;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.elasticsearch.river.subversion.beans.SubversionDocument;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static java.lang.Thread.currentThread;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SubversionDocumentTest {

    SubversionDocument document1;
    SubversionDocument document2;

    @SuppressWarnings("ConstantConditions")
    @Before
    public void setUp() throws Exception {
        File myTestFile1;
        File myTestFile2;
        myTestFile1 = new File(currentThread().getContextClassLoader().getResource("document1.json").toURI());
        myTestFile2 = new File(currentThread().getContextClassLoader().getResource("document2.json").toURI());
        Gson gson = new Gson();
        document1 = gson.fromJson(Files.newReader(myTestFile1, Charsets.UTF_8),
                SubversionDocument.class);
        document2 = gson.fromJson(Files.newReader(myTestFile2, Charsets.UTF_8),
                SubversionDocument.class);
    }

    @Test
    public void testJson() throws Exception {
        System.out.println(document1.json());
        assertNotNull(document1.json());
    }

    @Test
    public void testId() throws Exception {
        System.out.println(document1.id());
        System.out.println(document2.id());
        assertTrue(document1.id().equalsIgnoreCase(document2.id()));
    }
}
