package org.elasticsearch.river.subversion;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.subversion.crawler.Parameters;
import org.elasticsearch.river.subversion.crawler.SubversionCrawler;
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

    protected ESLogger logger = ESLoggerFactory.getLogger(
            SubversionCrawlerHTTPTest.class.getName());

    private URL reposAsURL;
    private Parameters parameters;

    @SuppressWarnings("ConstantConditions")
    @Before
    public void setUp() throws URISyntaxException, MalformedURLException {
        reposAsURL = new URL("http://google-code-prettify.googlecode.com/svn");
        parameters = new Parameters.ParametersBuilder()
            .setPath("/trunk")
        .create();
    }

    @Test
    public void testGetLatestRevision() throws SVNException, URISyntaxException {
        long revision = SubversionCrawler.getLatestRevision(reposAsURL, parameters);

        logger.info("Latest revision of [{}] is[{}]",
            reposAsURL +parameters.getPath().get(), revision
        );
        Assert.assertTrue(revision > 0);
    }

}
