package org.elasticsearch.river.subversion;

import org.elasticsearch.river.subversion.beans.SubversionDocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.svn.core.SVNDirEntry;
import org.tmatesoft.svn.core.SVNException;
import org.tmatesoft.svn.core.SVNURL;
import org.tmatesoft.svn.core.internal.io.fs.FSRepositoryFactory;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.io.SVNRepositoryFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.river.subversion.SubversionCrawler.*;

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
        path = "/";
    }

    @Test
    public void testSvnList() throws SVNException {
        List<String> result = SvnList(repos, path, null);
        for( String svnElementPath:result ) {
            System.out.println(svnElementPath);
        }

        Assert.assertTrue("This repository has normally 2 elements",result.size() == 2);
    }

    @Test
    public void testGetDocuments() throws SVNException {
        List<String> elementsList = SvnList(repos, path, null);
        List<SubversionDocument> result = getDocuments(elementsList, repos, -1L);
        for( SubversionDocument document : result ) {
            System.out.println(document.json());
        }

        Assert.assertTrue("This repository has normally 2 elements",result.size() == 2);
    }

    @Test
    public void testGetContent() throws SVNException {
        FSRepositoryFactory.setup();
        SVNRepository repository;
        repository = SVNRepositoryFactory.create(SVNURL.fromFile(repos));
        SVNDirEntry entry = repository.info("module1/trunk/watchlist.txt", 7L);
        String result = getContent(entry, repository);
        Assert.assertNotNull("This file cannot be empty/null", result);
    }

    @Test
    public void testGetLatestRevision() throws SVNException {
        long revision = SubversionCrawler.getLatestRevision(repos,path);

        System.out.println("Latest revision of "+repos+"/"+path+" == "+revision);
        Assert.assertTrue(revision > 0);
    }
}
