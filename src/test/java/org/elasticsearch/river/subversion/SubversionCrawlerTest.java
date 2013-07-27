package org.elasticsearch.river.subversion;

import com.google.common.base.Optional;
import org.elasticsearch.river.subversion.beans.SubversionRevision;
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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import static org.elasticsearch.river.subversion.SubversionCrawler.getContent;
import static org.elasticsearch.river.subversion.SubversionCrawler.getRevisions;


/**
 * Test for SVN SubversionCrawler
 */
public class SubversionCrawlerTest {

    private URL reposAsURL;
    private String path;

    @SuppressWarnings("ConstantConditions")
    @Before
    public void setUp() throws URISyntaxException, MalformedURLException {
        reposAsURL = Thread.currentThread().getContextClassLoader()
                .getResource("TEST_REPOS").toURI().toURL();
        path = "/";
    }

    @Test
    public void testGetRevisions() throws SVNException, URISyntaxException {
        List<SubversionRevision> result = getRevisions(
                reposAsURL,
                "",
                path,
                Optional.<Long>absent(),
                Optional.<Long>absent()
        );
        for( SubversionRevision revision : result ) {
            System.out.println(revision.json());
        }

        Assert.assertTrue("This repository has normally 7 revisions",result.size() == 7);
    }

    @Test
    public void testGetRevisionsModule1() throws SVNException, URISyntaxException {
        List<SubversionRevision> result = getRevisions(
                reposAsURL,
                "",
                "/module1",
                Optional.<Long>absent(),
                Optional.<Long>absent()
        );
        for( SubversionRevision revision : result ) {
            System.out.println(revision.json());
        }

        Assert.assertTrue("This repository has normally 5 revisions",result.size() == 5);
    }

    @Test
    public void testGetContent() throws SVNException, URISyntaxException {
        FSRepositoryFactory.setup();
        SVNRepository repository;
        repository = SVNRepositoryFactory.create(
                SVNURL.fromFile(new File(reposAsURL.toURI()))
        );
        SVNDirEntry entry = repository.info("module1/trunk/watchlist.txt", 7L);
        String result = getContent(entry, repository);
        Assert.assertNotNull("This file cannot be empty/null", result);
    }

    @Test
    public void testGetLatestRevision() throws SVNException, URISyntaxException {
        long revision = SubversionCrawler.getLatestRevision(reposAsURL, "",path);

        System.out.println("Latest revision of "+ reposAsURL +"/"+path+" == "+revision);
        Assert.assertTrue(revision > 0);
    }

}
