package org.elasticsearch.river.subversion;

import com.google.common.base.Charsets;
import org.tmatesoft.svn.core.*;
import org.tmatesoft.svn.core.internal.io.fs.FSRepositoryFactory;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.io.SVNRepositoryFactory;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Container for SVN repository browsing
 */
public class Browser {

    public static List<SVNDocument> SvnList(String repos, String path) {
        List<SVNDocument> result = new ArrayList<SVNDocument>();
        FSRepositoryFactory.setup();
        SVNRepository repository;
        try {
            repository = SVNRepositoryFactory.create(SVNURL.parseURIDecoded(repos));
            System.out.println( "Repository Root: " + repository.getRepositoryRoot(true) );
            System.out.println(  "Repository UUID: " + repository.getRepositoryUUID(true) );
            listEntriesRecursive(repository, path, result);
        } catch (SVNException e) {
            e.printStackTrace();
        }

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
    public static void listEntriesRecursive( SVNRepository repository, String path, List<SVNDocument> list ) throws SVNException {
         for(SVNDirEntry entry:(Collection<SVNDirEntry>)repository.getDir( path, -1 , null , (Collection) null )) {
            if ( entry.getKind() == SVNNodeKind.DIR ) {
                listEntriesRecursive(repository, (path.equals("")) ? entry.getName() : path + "/" + entry.getName(), list);
            } else {
                SVNDocument svnDocument = new SVNDocument(entry, repository, path);
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
        ByteArrayOutputStream baos = new ByteArrayOutputStream( );
        String filePath = path+"/"+entry.getRelativePath();

        try {
            SVNNodeKind kind = repository.checkPath( filePath , entry.getRevision());
            // if the kind is "none", file simply does not exist
            if(!kind.equals(SVNNodeKind.FILE)) {
                return null;
            }
            repository.getFile( filePath , entry.getRevision() , fileProperties , baos );
            String mimeType = fileProperties.getStringValue(SVNProperty.MIME_TYPE);
            boolean isTextType = SVNProperty.isTextMimeType( mimeType );
            if(isTextType) {
                content = baos.toString(Charsets.UTF_8.name());
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
