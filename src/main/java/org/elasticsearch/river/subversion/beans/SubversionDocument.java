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

package org.elasticsearch.river.subversion.beans;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.elasticsearch.river.subversion.SubversionCrawler;
import org.tmatesoft.svn.core.SVNDirEntry;
import org.tmatesoft.svn.core.io.SVNRepository;

/**
 * JavaBean for handling JSON generation from SVNEntries
 * For the moment, does not handle directories, only files.
 * TODO : handle directories, with changed paths.
 */
@SuppressWarnings("unused")
public class SubversionDocument {

    @Expose final String path;
    @Expose final String name;
    @Expose final long size;
    @Expose final char change;
    @Expose final String content;

    public SubversionDocument(SVNDirEntry entry, SVNRepository repository, char change) {
        this.path = entry.getURL().toDecodedString();
        this.content = SubversionCrawler.getContent(entry, repository);
        this.name = entry.getName();
        this.size = entry.getSize();
        this.change = change;
    }

    public String json() {
        Gson gson = new GsonBuilder()
                .excludeFieldsWithoutExposeAnnotation()
                .setDateFormat(SubversionRevision.DATE_TIME_ISO8601_FORMAT)
                .create();
        return gson.toJson(this);
    }
}
