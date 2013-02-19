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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.tmatesoft.svn.core.SVNDirEntry;
import org.tmatesoft.svn.core.io.SVNRepository;

import java.util.Date;

/**
 * JavaBean for handling JSON generation from SVNEntries
 * For the moment, does not handle directories, only files.
 * TODO : handle directories, with changed paths.
 */
@SuppressWarnings("unused")
public class SubversionDocument {

    @Expose final String path;
    @Expose final String name;
    @Expose final String author;
    @Expose final String repository;
    @Expose final long revision;
    @Expose final Date date;
    @Expose final long size;
    @Expose final String message;
    @Expose final String content;

    private static final HashFunction hf = Hashing.md5();


    public SubversionDocument(SVNDirEntry entry, SVNRepository repository, String path) {
        this.path = entry.getURL().toDecodedString();
        this.author = entry.getAuthor();
        this.content = SubversionCrawler.getContent(entry, repository, path);
        this.date = entry.getDate();
        this.message = entry.getCommitMessage();
        this.name = entry.getName();
        this.revision = entry.getRevision();
        this.size = entry.getSize();
        this.repository = entry.getRepositoryRoot().getPath();
    }

    public String json() {
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        return gson.toJson(this);
    }

    @Override
    public String toString() {
        return json();
    }

    /**
     * Path should be sufficient to uniquely identify a file@revision
     * @return  a loosely constructed hashcode converted to String
     */
    public String id() {
        return hf.newHasher()
                .putString(path)
                .hash()
                .toString();
    }

}
