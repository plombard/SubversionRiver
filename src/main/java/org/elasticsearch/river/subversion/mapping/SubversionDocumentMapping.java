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
import org.elasticsearch.river.subversion.bean.SubversionDocument;
import org.elasticsearch.river.subversion.bean.SubversionRevision;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Mapping for the indexing of SubversionRevision objects
 */
@SuppressWarnings("unused")
public class SubversionDocumentMapping {

    private static XContentBuilder instance;

    public static XContentBuilder getInstance() throws IOException {
        if( instance == null) {
            instance = jsonBuilder().startObject()
                        .startObject(SubversionDocument.TYPE_NAME)
                            .startObject("_parent")
                                .field("type", SubversionRevision.TYPE_NAME)
                            .endObject()
                            .startObject("properties")
                                .startObject("path")
                                    .field("type", "multi_field")
                                    .startObject("fields")
                                        .startObject("path")
                                            .field("type", "string")
                                            .field("index", "analyzed")
                                        .endObject()
                                        .startObject("untouched")
                                            .field("type", "string")
                                            .field("index", "not_analyzed")
                                        .endObject()
                                    .endObject()
                                .endObject()
                                .startObject("name")
                                    .field("type", "multi_field")
                                    .startObject("fields")
                                        .startObject("name")
                                            .field("type", "string")
                                            .field("index", "analyzed")
                                        .endObject()
                                        .startObject("untouched")
                                            .field("type", "string")
                                            .field("index", "not_analyzed")
                                        .endObject()
                                    .endObject()
                                .endObject()
                                .startObject("size")
                                    .field("type", "integer")
                                    .field("index", "not_analyzed")
                                .endObject()
                                .startObject("change")
                                    .field("type", "string")
                                    .field("index", "not_analyzed")
                                .endObject()
                                .startObject("content")
                                    .field("type", "string")
                                    .field("index", "analyzed")
                                .endObject()
                                .startObject("from")
                                    .field("type", "long")
                                    .field("index", "not_analyzed")
                                .endObject()
                                .startObject("origin")
                                    .field("type", "multi_field")
                                    .startObject("fields")
                                        .startObject("origin")
                                            .field("type", "string")
                                            .field("index", "analyzed")
                                        .endObject()
                                        .startObject("untouched")
                                            .field("type", "string")
                                            .field("index", "not_analyzed")
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
            .endObject();
        }

        return instance;
    }
}
