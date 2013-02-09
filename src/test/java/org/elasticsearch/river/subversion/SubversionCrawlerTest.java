package org.elasticsearch.river.subversion;

import junit.framework.TestCase;

import java.util.List;

/**
 * Test for SVN SubversionCrawler
 */
public class SubversionCrawlerTest extends TestCase {
    public void testSvnList() throws Exception {
        String repos = "file:///D:/repos/0D00";
        String path = "ReferencesCroisees/trunk";
        List<SubversionDocument> result = SubversionCrawler.SvnList(repos, path);
        for( SubversionDocument svnDocument:result ) {
            System.out.println(svnDocument.json());
            System.out.println("");
        }

        System.out.println(result.size());
    }
}
