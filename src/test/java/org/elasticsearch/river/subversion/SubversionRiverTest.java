package org.elasticsearch.river.subversion;

import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchAdminClient;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchClient;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchIndex;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchNode;
import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RunWith(ElasticsearchRunner.class)
@ElasticsearchNode
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SubversionRiverTest {

    @ElasticsearchClient()
    Client client1;

    @ElasticsearchAdminClient
    AdminClient adminClient;

    @Test
    @ElasticsearchIndex(indexName = "_river")
    public void testCreateIndex() {
        // Checks if the index has been created
        IndicesExistsResponse existResponse = adminClient.indices()
                .prepareExists("_river")
                .execute().actionGet();

        Assert.assertTrue("Index must exist", existResponse.exists());
    }

    @Test
    @ElasticsearchIndex(indexName = "_river")
    public void testIndexing() throws IOException {
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

        Assert.assertTrue("Indexing must return a version >= 1",
                indexResponse.version() >= 1);
    }

    @Test
    @ElasticsearchIndex(indexName = "_river")
    public void testSearching() {
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

        for(SearchHit hit : searchResponse.hits()) {
            System.out.println("Search result :"+hit.sourceAsString());
        }

        Assert.assertTrue("There must be a watchlist.txt in the repository",
                searchResponse.hits().totalHits() > 0);
    }

}
