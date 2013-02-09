package org.elasticsearch.river.subversion;

import com.google.common.base.Charsets;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.tmatesoft.svn.core.*;
import org.tmatesoft.svn.core.internal.io.fs.FSRepositoryFactory;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.io.SVNRepositoryFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Container for SVN repository browsing
 */
public class SubversionCrawler {

    private static ESLogger logger = Loggers.getLogger(SubversionCrawler.class);

    public static long getLastRevision(String repos, String path) throws SVNException {
        long result;
        FSRepositoryFactory.setup();
        SVNRepository repository;
        repository = SVNRepositoryFactory.create(SVNURL.parseURIDecoded(repos));
        logger.info( "Repository Root: " + repository.getRepositoryRoot(true) );
        logger.info(  "Repository UUID: " + repository.getRepositoryUUID(true) );
        logger.info(  "Repository HEAD Revision: " + repository.getLatestRevision() );

        return repository.getLatestRevision();
    }

    public static List<SubversionDocument> SvnList(String repos, String path) throws SVNException {
        List<SubversionDocument> result = new ArrayList<SubversionDocument>();
        FSRepositoryFactory.setup();
        SVNRepository repository;
        repository = SVNRepositoryFactory.create(SVNURL.parseURIDecoded(repos));
        logger.debug( "Repository Root: " + repository.getRepositoryRoot(true) );
        logger.debug(  "Repository UUID: " + repository.getRepositoryUUID(true) );

        listEntriesRecursive(repository, path, result);

        return result;
    }


    /**
     * Straight from SVNKit tutorial :D
     *
     * @param repository  repos to explore
     * @param path starting path
     * @throws SVNException
     */
    @SuppressWarnings("unchecked")
    public static void listEntriesRecursive( SVNRepository repository, String path, List<SubversionDocument> list ) throws SVNException {
         for(SVNDirEntry entry:(Collection<SVNDirEntry>)repository.getDir( path, -1 , null , (Collection) null )) {
            if ( entry.getKind() == SVNNodeKind.DIR ) {
                listEntriesRecursive(repository, (path.equals("")) ? entry.getName() : path + "/" + entry.getName(), list);
            } else {
                SubversionDocument svnDocument = new SubversionDocument(entry, repository, path);
                list.add(svnDocument);
            }
        }
    }

    /**
     *  Get the SVNEntry file content
     * @param entry the SVNEntry
     * @param repository  the repository containing the entry
     * @param path  the relative path of the entry
     * @return the text content of the file, or null if exception or not a file
     */
    // TODO: Sanitize this method, properly escape the content, check on encoding, visibility...
    public static String getContent(SVNDirEntry entry, SVNRepository repository, String path) {
        String content = null;
        // Only applies to files
        if(entry.getKind() != SVNNodeKind.FILE) {
            return null;
        }
        SVNProperties fileProperties = new SVNProperties();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        String filePath = path.concat(File.separator).concat(entry.getRelativePath());

        try {
            SVNNodeKind kind = repository.checkPath( filePath , entry.getRevision());
            // if the kind is "none", file simply does not exist
            if(!kind.equals(SVNNodeKind.FILE)) {
                return null;
            }
            repository.getFile( filePath , entry.getRevision() , fileProperties , outputStream );
            String mimeType = fileProperties.getStringValue(SVNProperty.MIME_TYPE);
            boolean isTextType = SVNProperty.isTextMimeType( mimeType );
            if(isTextType) {
                content = outputStream.toString(Charsets.UTF_8.name());
            }

        } catch (SVNException e) {
            e.printStackTrace();
            return null;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }

        return content;
    }

}
