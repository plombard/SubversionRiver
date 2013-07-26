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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Mapping for the indexing of the IndexedRevisionRevision object
 * It has to be a long, and the automatic mapping always convert it to integer
 */
@SuppressWarnings("unused")
public class IndexedRevisionMapping {

    private static XContentBuilder instance;

    public static XContentBuilder getInstance() throws IOException {
        if( instance == null) {
            instance = jsonBuilder().startObject().startObject("indexed_revision")
                    .startObject("properties")
                        .startObject("revision")
                            .field("type", "long")
                            .field("index", "analyzed")
                        .endObject()
                    .endObject()
            .endObject().endObject();
        }

        return instance;
    }
}
