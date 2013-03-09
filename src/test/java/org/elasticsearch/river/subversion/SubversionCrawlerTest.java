package org.elasticsearch.river.subversion;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.svn.core.SVNException;
import org.tmatesoft.svn.core.SVNURL;
import org.tmatesoft.svn.core.internal.io.fs.FSRepositoryFactory;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.io.SVNRepositoryFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.river.subversion.SubversionCrawler.SvnList;

/**
 * Test for SVN SubversionCrawler
 */
public class SubversionCrawlerTest {

    private File repos;
    private String path;

    @SuppressWarnings("ConstantConditions")
    @Before
    public void setUp() throws URISyntaxException {
        repos = new File(Thread.currentThread().getContextClassLoader()
                .getResource("TEST_REPOS").toURI());
        path = "module1/trunk";
    }

    @Test
    public void testSvnList() throws Exception {
        List<SubversionDocument> result = SvnList(repos, path, null);
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

    @Test
    public void testListEntriesByRevision() throws  SVNException {
        long revision = 5L;
        List<SubversionDocument> result = new ArrayList<SubversionDocument>();
        FSRepositoryFactory.setup();
        SVNRepository repository;
        repository = SVNRepositoryFactory.create(SVNURL.fromFile(repos));

        Iterable<SubversionDocument> documents =
                SubversionCrawler.listEntriesByRevision(repository, revision);

        System.out.println(documents);
    }
}
