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
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.river.subversion.beans.SubversionDocument;
import org.elasticsearch.river.subversion.beans.SubversionRevision;
import org.tmatesoft.svn.core.*;
import org.tmatesoft.svn.core.internal.io.fs.FSRepositoryFactory;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.io.SVNRepositoryFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Container for SVN repository browsing
 */
public class SubversionCrawler {

    private static ESLogger logger = Loggers.getLogger(SubversionCrawler.class);

    /**
     * Return the latest revision of a SVN directory
     *
     * @param repos repository
     * @param path  path to the directory
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
        return repository.getDir(path, -1, false, null).getRevision();
    }

    public static List<SubversionRevision> getRevisions(File repos,
                                                        String path,
                                                        Optional<Long> startOp,
                                                        Optional<Long> endOp)
            throws SVNException {
        List<SubversionRevision> result = Lists.newArrayList();
        // Init the first revision to get
        Long start = startOp.isPresent() ? startOp.get() : 1;
        // Init the last revision to get
        FSRepositoryFactory.setup();
        SVNRepository repository;
        repository = SVNRepositoryFactory.create(SVNURL.fromFile(repos));
        logger.debug("Repository Root: " + repository.getRepositoryRoot(true));
        logger.debug("Repository UUID: " + repository.getRepositoryUUID(true));
        Long end = endOp.isPresent() ? endOp.get() : repository.getLatestRevision();

        String[] targetPaths = new String[1];
        targetPaths[0] = path;

        // For every revision in the range
        Collection logEntries =
                repository.log(
                        targetPaths,
                        null,
                        start,
                        end,
                        true,
                        true
                );
        for (Object logEntryObject : logEntries) {
            SVNLogEntry logEntry = (SVNLogEntry) logEntryObject;
            // Map the obtained logEntry to the jsonable/indexable class
            SubversionRevision subversionRevision =
                    new SubversionRevision(logEntry, repository.getLocation().getPath());

            final Map<String, SVNLogEntryPath> changedPaths = logEntry.getChangedPaths();

            for (Map.Entry<String, SVNLogEntryPath> entry : changedPaths.entrySet()) {
                // For each changed path, get the SVNDirEntry...
                SVNLogEntryPath svnLogEntryPath = entry.getValue();
                SVNDirEntry dirEntry = repository.getDir(
                        svnLogEntryPath.getPath(),
                        logEntry.getRevision(),
                        false,
                        null
                );

                // ...and init a SubversionDocument to add to the revision
                subversionRevision.addDocument(
                        new SubversionDocument(
                                dirEntry,
                                repository,
                                svnLogEntryPath.getType()
                        )
                );
            }
            result.add(subversionRevision);
        }
        return result;
    }

    /**
     * Get the SVNEntry file content
     *
     * @param entry      the SVNEntry
     * @param repository the repository containing the entry
     * @return the text content of the file, or null if exception or not a file
     */
    // TODO: Sanitize this method, properly escape the content, check on encoding, visibility...
    public static String getContent(SVNDirEntry entry, SVNRepository repository) {
        String content;
        // Only applies to files
        if (entry.getKind() != SVNNodeKind.FILE) {
            return null;
        }

        // A terrible way to find the entry path relative to the repository root
        String path = entry.getURL().toString().replaceFirst(entry.getRepositoryRoot().toString(), "");
        SVNProperties fileProperties = new SVNProperties();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            SVNNodeKind kind = repository.checkPath(path, entry.getRevision());
            // if the kind is "none", file simply does not exist
            if (!kind.equals(SVNNodeKind.FILE)) {
                return null;
            }
            repository.getFile(path, entry.getRevision(), fileProperties, outputStream);
            String mimeType = fileProperties.getStringValue(SVNProperty.MIME_TYPE);
            boolean isTextType = SVNProperty.isTextMimeType(mimeType);
            if (isTextType) {
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
