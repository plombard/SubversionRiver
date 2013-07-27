package org.elasticsearch.river.subversion;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tmatesoft.svn.core.SVNException;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;


/**
 * Test for SVN SubversionCrawler
 */
public class SubversionCrawlerHTTPTest {

    private URL reposAsURL;
    private String userInfo;
    private String path;

    @SuppressWarnings("ConstantConditions")
    @Before
    public void setUp() throws URISyntaxException, MalformedURLException {
        reposAsURL = new URL("http://google-code-prettify.googlecode.com/svn");
        path = "/trunk";
        userInfo = "anonymous:password";
    }

    @Test
    public void testGetLatestRevision() throws SVNException, URISyntaxException {
        long revision = SubversionCrawler.getLatestRevision(reposAsURL, userInfo,path);

        System.out.println("Latest revision of "+ reposAsURL +path+" == "+revision);
        Assert.assertTrue(revision > 0);
    }

}
