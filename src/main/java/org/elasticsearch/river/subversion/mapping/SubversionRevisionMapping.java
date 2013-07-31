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

package org.elasticsearch.river.subversion.mapping;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.river.subversion.bean.SubversionRevision;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Mapping for the indexing of SubversionRevision objects
 */
@SuppressWarnings("unused")
public class SubversionRevisionMapping {

    private static XContentBuilder instance;

    public static XContentBuilder getInstance() throws IOException {
        if( instance == null) {
            instance = jsonBuilder().startObject().startObject(SubversionRevision.TYPE_NAME)
                    .startObject("properties")
                        .startObject("author")
                            .field("type", "multi_field")
                            .startObject("fields")
                                .startObject("author")
                                    .field("type", "string")
                                    .field("index", "analyzed")
                                .endObject()
                                .startObject("untouched")
                                    .field("type", "string")
                                    .field("index", "not_analyzed")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("repository")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("revision")
                            .field("type", "long")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("date")
                            .field("type", "date")
                            .field("format", "date_time")
                            .field("index", "analyzed")
                        .endObject()
                        .startObject("message")
                            .field("type", "string")
                            .field("index", "analyzed")
                        .endObject()
                    .endObject()
            .endObject().endObject();
        }

        return instance;
    }
}
