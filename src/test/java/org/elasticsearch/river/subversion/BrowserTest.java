package org.elasticsearch.river.subversion;

import junit.framework.TestCase;

import java.util.List;

/**
 * Test for SVN Browser
 */
public class BrowserTest extends TestCase{
    public void testSvnList() throws Exception {
        String repos = "file:///D:/repos/test";
        String path = "myproject/trunk";
        List<SVNDocument> result = Browser.SvnList(repos,path);
        for( SVNDocument svnDocument:result ) {
            System.out.println(svnDocument.json());
            System.out.println("");
        }

        System.out.println(result.size());
    }
}
