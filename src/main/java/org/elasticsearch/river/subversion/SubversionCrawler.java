/*
 * Copyright [2013] [Pascal Lombard]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.elasticsearch.river.subversion;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.tmatesoft.svn.core.*;
import org.tmatesoft.svn.core.internal.io.fs.FSRepositoryFactory;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.io.SVNRepositoryFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
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
     * Find and retrieve the elements paths under the svn path specified.
     * @param repos repository
     * @param path svn path to inspect
     * @param revision revision to retrieve, leave null or -1 for HEAD
     * @return the list of items in the svn path at the specified revision
     * @throws SVNException
     */
    public static List<String> SvnList(File repos, String path, Long revision) throws SVNException {
        List<String> result = Lists.newArrayList();
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
     * @param list list of elements to populate
     * @param revision revision to fetch
     * @throws SVNException
     */
    @SuppressWarnings("unchecked")
    private static void listEntriesRecursive( SVNRepository repository, String path, List<String> list, Long revision ) throws SVNException {
         for(SVNDirEntry entry:(Collection<SVNDirEntry>)repository.getDir( path, revision , null , (Collection) null )) {
            if ( entry.getKind() == SVNNodeKind.DIR ) {
                listEntriesRecursive(repository,
                        (path.equals("")) ? entry.getName() : path .concat("/") .concat(entry.getName()),
                        list,
                        revision
                );
            } else {
                list.add(path .concat("/") .concat(entry.getName()));
            }
        }
    }

    /**
     * Create a SubversionDocument with the element at the revision.
     * @param repository the repository containing the element.
     * @param svnElementPath the full path of the element, including the element.
     * @param revision the revision to consider.
     * @return a complete SubversionDocument
     */
    private static SubversionDocument getDocument(SVNRepository repository, String svnElementPath, Long revision) throws SVNException {
        return new SubversionDocument(
                    repository.info(svnElementPath, revision),
                    repository
                );
    }

    /**
     * Fill up a list of SubversionDocument from a list of elements
     * @param elements relative paths of the elements
     * @param repos subversion repository
     * @param revision the revision to consider
     * @return a list of the corresponding documents
     * @throws SVNException
     */
    public static List<SubversionDocument> getDocuments(List<String> elements, File repos, Long revision)
            throws SVNException {
        FSRepositoryFactory.setup();
        SVNRepository repository;
        repository = SVNRepositoryFactory.create(SVNURL.fromFile(repos));
        logger.debug( "Repository Root: " + repository.getRepositoryRoot(true) );
        logger.debug(  "Repository UUID: " + repository.getRepositoryUUID(true) );

        List<SubversionDocument> result = Lists.newArrayList();
        for(String element : elements) {
            result.add(getDocument(repository, element, revision));
        }
        return result;
    }

    /**
     *  Get the SVNEntry file content
     * @param entry the SVNEntry
     * @param repository  the repository containing the entry
     * @return the text content of the file, or null if exception or not a file
     */
    // TODO: Sanitize this method, properly escape the content, check on encoding, visibility...
    public static String getContent(SVNDirEntry entry, SVNRepository repository) {
        String content;
        // Only applies to files
        if(entry.getKind() != SVNNodeKind.FILE) {
            return null;
        }

        // A terrible way to find the entry path relative to the repository root
        String path = entry.getURL().toString().replaceFirst(entry.getRepositoryRoot().toString(),"");
        SVNProperties fileProperties = new SVNProperties();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            SVNNodeKind kind = repository.checkPath( path , entry.getRevision());
            // if the kind is "none", file simply does not exist
            if(!kind.equals(SVNNodeKind.FILE)) {
                return null;
            }
            repository.getFile( path , entry.getRevision() , fileProperties , outputStream );
            String mimeType = fileProperties.getStringValue(SVNProperty.MIME_TYPE);
            boolean isTextType = SVNProperty.isTextMimeType( mimeType );
            if(isTextType) {
                content = outputStream.toString(Charsets.UTF_8.name());
            } else {
                content = "Not text type";
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
