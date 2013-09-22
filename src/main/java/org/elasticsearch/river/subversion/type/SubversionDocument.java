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

package org.elasticsearch.river.subversion.type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.elasticsearch.river.subversion.SubversionCrawler;
import org.tmatesoft.svn.core.SVNDirEntry;
import org.tmatesoft.svn.core.SVNException;
import org.tmatesoft.svn.core.SVNLogEntryPath;
import org.tmatesoft.svn.core.io.SVNRepository;

import java.util.Date;

/**
 * JavaBean for handling JSON generation from SVNEntries
 * For the moment, does not handle directories, only files.
 * TODO : handle directories, with changed paths.
 */
@SuppressWarnings("unused")
public class SubversionDocument {

    @Expose final String path;       // File path
    @Expose final String name;       // File name
    @Expose final String fullname;   // Full File name
    @Expose final long size;         // File size
    @Expose final char change;       // Type of change
    @Expose final String content;    // File content
    @Expose final long from;         // Parent revision
    @Expose final String origin;     // Parent path
    @Expose final String author;     // Comitter
    @Expose final String repository; // File repository
    @Expose final long revision;     // revision number
    @Expose final Date date;         // Commit date
    @Expose final String message;    // Commit message

    public static final String TYPE_NAME = "svndocument";

    public SubversionDocument(SVNLogEntryPath entryPath,
                              SVNRepository repository,
                              long revisionNumber,
                              SubversionRevision revision)
            throws SVNException {
        this.path = entryPath.getPath().substring(0, entryPath.getPath().lastIndexOf("/"));
        this.fullname = entryPath.getPath();
        this.change = entryPath.getType();
        this.origin = entryPath.getCopyPath();
        this.from = entryPath.getCopyRevision();
        this.author = revision.author;
        this.repository = revision.repository;
        this.revision = revision.revision;
        this.date = revision.date;
        this.message = revision.message;
        // First check the type of the changement ofthe entry,
        // for it implies which type of info
        // we'll be able to extract.
        // If the path was added or modified,
        // we'll get a DirEntry
        if (change == 'A'
                || change == 'M') {
            SVNDirEntry dirEntry = repository.info(
                    entryPath.getPath(),
                    revisionNumber
            );
            // ...and init a SubversionDocument to add to the revision
            this.content = SubversionCrawler.getContent(dirEntry, repository);
            this.name = dirEntry.getName();
            this.size = dirEntry.getSize();
        } else {
            // Else, the entry was deleted or replaced
            // So we can't getDir() on it,
            // and the content, size, etc are irrelevant.
            this.content = null;
            this.name = entryPath.getPath().substring(entryPath.getPath().lastIndexOf("/"));
            this.size = 0;
        }
    }

    public String json() {
        Gson gson = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation()
                .setDateFormat(SubversionRevision.DATE_TIME_ISO8601_FORMAT)
                .create();
        return gson.toJson(this);
    }
}
