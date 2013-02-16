package org.elasticsearch.river.subversion;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
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

    /**
     * Return the latest revision of a SVN directory
     * @param repos repository
     * @param path path to the directory
     * @return latest revision
     * @throws SVNException
     */
    public static long getLatestRevision(File repos, String path) throws SVNException {
        FSRepositoryFactory.setup();
        SVNRepository repository;
        repository = SVNRepositoryFactory.create(SVNURL.fromFile(repos));
        logger.debug("Repository Root: " + repository.getRepositoryRoot(true));
        logger.debug("Repository UUID: " + repository.getRepositoryUUID(true));
        logger.debug("Repository HEAD Revision: " + repository.getLatestRevision());

        // call getDir() at HEAD revision,
        // no commit messages or entries necessary
        return repository.getDir(path,-1,false,null).getRevision();
    }

    /**
     * Find and retrieve the elements under the svn path specified.
     * @param repos repository
     * @param path svn path to inspect
     * @param revision revision to retrieve, leave null or -1 for HEAD
     * @return the list of items in the svn path at the specified revision
     * @throws SVNException
     */
    public static List<SubversionDocument> SvnList(File repos, String path, Long revision) throws SVNException {
        List<SubversionDocument> result = new ArrayList<SubversionDocument>();
        FSRepositoryFactory.setup();
        SVNRepository repository;
        repository = SVNRepositoryFactory.create(SVNURL.fromFile(repos));
        logger.debug( "Repository Root: " + repository.getRepositoryRoot(true) );
        logger.debug(  "Repository UUID: " + repository.getRepositoryUUID(true) );

        // list entries at specified revision,
        // or HEAD if revision is null
        listEntriesRecursive(repository, path, result, Objects.firstNonNull(revision, (long) -1));

        return result;
    }


    /**
     * Straight from SVNKit tutorial :D
     *
     * @param repository  repos to explore
     * @param path starting path
     * @param list SVNDocument list to populate
     * @param revision revision to fetch
     * @throws SVNException
     */
    @SuppressWarnings("unchecked")
    private static void listEntriesRecursive( SVNRepository repository, String path, List<SubversionDocument> list, Long revision ) throws SVNException {
         for(SVNDirEntry entry:(Collection<SVNDirEntry>)repository.getDir( path, revision , null , (Collection) null )) {
            if ( entry.getKind() == SVNNodeKind.DIR ) {
                listEntriesRecursive(repository, (path.equals("")) ? entry.getName() : path + "/" + entry.getName(), list, revision);
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
