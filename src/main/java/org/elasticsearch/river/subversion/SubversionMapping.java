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

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * Mapping for the indexing of SubversionDocument objects
 */
@SuppressWarnings("unused")
public class SubversionMapping {

    private static XContentBuilder instance;

    public static XContentBuilder getInstance() throws IOException {
        if( instance == null) {
            instance = jsonBuilder().startObject().startObject("svn")
                    .startObject("properties")
                        .startObject("path")
                            .field("type", "string")
                        .endObject()
                        .startObject("name")
                            .field("type", "string")
                        .endObject()
                        .startObject("author")
                            .field("type", "string")
                        .endObject()
                        .startObject("repository")
                            .field("type", "string")
                        .endObject()
                        .startObject("revision")
                            .field("type", "long")
                        .endObject()
                        .startObject("date")
                            .field("type", "date")
                            .field("format", "date_time")
                        .endObject()
                        .startObject("size")
                            .field("type", "long")
                        .endObject()
                        .startObject("message")
                            .field("type", "string")
                        .endObject()
                        .startObject("content")
                            .field("type", "string")
                        .endObject()
                    .endObject()
            .endObject().endObject();
        }

        return instance;
    }
}
