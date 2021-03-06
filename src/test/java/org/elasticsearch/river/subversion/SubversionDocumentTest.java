package org.elasticsearch.river.subversion;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.subversion.type.SubversionDocument;
import org.elasticsearch.river.subversion.type.SubversionRevision;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static java.lang.Thread.currentThread;
import static org.junit.Assert.assertTrue;

public class SubversionDocumentTest {

    protected ESLogger logger = ESLoggerFactory.getLogger(SubversionDocumentTest.class.getName());

    SubversionDocument document;
    SubversionRevision revision;

    @SuppressWarnings("ConstantConditions")
    @Before
    public void setUp() throws Exception {
        File myTestFile1;
        File myTestFile2;
        myTestFile1 = new File(currentThread().getContextClassLoader()
                .getResource("document.json").toURI());
        myTestFile2 = new File(currentThread().getContextClassLoader()
                .getResource("revision.json").toURI());
        Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create();
        document = gson.fromJson(Files.newReader(myTestFile1, Charsets.UTF_8),
                SubversionDocument.class);
        revision = gson.fromJson(Files.newReader(myTestFile2, Charsets.UTF_8),
                SubversionRevision.class);
    }

    @Test
    public void testDocumentJson() throws Exception {
        logger.info(document.json());
        assertTrue(
                "Document must contain Metal Gear",
                document.json().contains("Metal Gear")
        );
    }

    @Test
    public void testRevisionJson() throws Exception {
        logger.info(revision.json());
        assertTrue(
                "Revision must contain hell",
                revision.json().contains("hell")
        );
    }


}
