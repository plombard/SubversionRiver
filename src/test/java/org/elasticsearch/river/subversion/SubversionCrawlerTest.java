package org.elasticsearch.river.subversion;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.svn.core.SVNException;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Test for SVN SubversionCrawler
 */
public class SubversionCrawlerTest {

    private File repos;
    private String path;

    @Before
    public void setUp() throws URISyntaxException {
        repos = new File(Thread.currentThread().getContextClassLoader()
                .getResource("TEST_REPOS").toURI());
        path = "module1/trunk";
    }

    @Test
    public void testSvnList() throws Exception {
        List<SubversionDocument> result = SubversionCrawler.SvnList(repos, path, null);
        for( SubversionDocument svnDocument:result ) {
            System.out.println(svnDocument.json());
            System.out.println("");
        }

        Assert.assertTrue(result.size() > 0);
    }

    @Test
    public void testGetLatestRevision() throws SVNException {
        long revision = SubversionCrawler.getLatestRevision(repos,path);

        System.out.println("Latest revision of "+repos+"/"+path+" == "+revision);
        Assert.assertTrue(revision > 0);
    }
}
