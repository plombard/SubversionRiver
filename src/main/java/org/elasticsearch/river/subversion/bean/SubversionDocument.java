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

package org.elasticsearch.river.subversion.bean;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.elasticsearch.river.subversion.SubversionCrawler;
import org.tmatesoft.svn.core.SVNDirEntry;
import org.tmatesoft.svn.core.SVNException;
import org.tmatesoft.svn.core.SVNLogEntryPath;
import org.tmatesoft.svn.core.io.SVNRepository;

import java.nio.file.Paths;

/**
 * JavaBean for handling JSON generation from SVNEntries
 * For the moment, does not handle directories, only files.
 * TODO : handle directories, with changed paths.
 */
@SuppressWarnings("unused")
public class SubversionDocument {

    @Expose final String path;    // File path
    @Expose final String name;    // File name
    @Expose final long size;      // File size
    @Expose final char change;    // Type of change
    @Expose final String content; // File content
    @Expose final long from;      // Parent revision
    @Expose final String origin;  // Parent path

    public static final String TYPE_NAME = "svndocument";

    public SubversionDocument(SVNLogEntryPath entryPath, SVNRepository repository, long revision)
            throws SVNException {
        this.path = Paths.get(entryPath.getPath()).getParent().toString();
        this.change = entryPath.getType();
        this.origin = entryPath.getCopyPath();
        this.from = entryPath.getCopyRevision();
        // First check the type of the changement ofthe entry,
        // for it implies which type of info
        // we'll be able to extract.
        // If the path was added or modified,
        // we'll get a DirEntry
        if (change == 'A'
                || change == 'M') {
            SVNDirEntry dirEntry = repository.info(
                    entryPath.getPath(),
                    revision
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
            this.name = Paths.get(
                    entryPath.getPath())
                    .getFileName()
                    .toString();
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
