package org.elasticsearch.river.subversion;

import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchClient;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchIndex;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchNode;
import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RunWith(ElasticsearchRunner.class)
@ElasticsearchNode
public class SubversionRiverTest {

    @ElasticsearchClient()
    Client client1;

    @Test
    @ElasticsearchIndex(indexName = "_river")
    public void testIndexingAndSearching() throws IOException {
        // Index a new document
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("repos","TEST_REPOS");
        json.put("path","/");

        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("type", "svn")
                .field("svn",json)
                .endObject();

        IndexResponse indexResponse = client1.prepareIndex("_river", "mysvnriver", "_meta")
                .setSource(builder)
                .execute()
                .actionGet();
        Assert.assertTrue(indexResponse.version() >= 1);

        // Wait 2s for the indexing to take place.
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        SearchResponse searchResponse = client1.prepareSearch()
                .setQuery(QueryBuilders.fieldQuery("name","watchlist.txt"))
                .execute()
                .actionGet();

        Assert.assertTrue(searchResponse.hits().totalHits() > 0);
    }

}
