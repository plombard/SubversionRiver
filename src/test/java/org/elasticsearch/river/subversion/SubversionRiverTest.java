package org.elasticsearch.river.subversion;

import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchAdminClient;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchClient;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchIndex;
import com.github.tlrx.elasticsearch.test.annotations.ElasticsearchNode;
import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.subversion.type.SubversionDocument;
import org.elasticsearch.search.SearchHit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RunWith(ElasticsearchRunner.class)
@ElasticsearchNode
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SubversionRiverTest {

    @ElasticsearchClient()
    Client client1;

    @ElasticsearchAdminClient
    AdminClient adminClient;

    private static final HashFunction hf = Hashing.md5();
    private static String REPOS;
    private static final String PATH = "/";
    private static final Long START_REVISION = 4L;
    private static final String RIVER_KEYWORD = "_river";
    private static String INDEXED_REVISION_ID;

    @SuppressWarnings("ConstantConditions")
    @Before
    public void setUp() throws URISyntaxException {
        REPOS = currentThread().getContextClassLoader()
                .getResource("TEST_REPOS")
                .toURI().toString();
        INDEXED_REVISION_ID =
                "_indexed_revision_".concat(
                        hf.newHasher()
                                .putUnencodedChars(REPOS)
                                .putUnencodedChars(PATH)
                                .hash()
                                .toString()
                );
    }

    @Test
    @ElasticsearchIndex(indexName = RIVER_KEYWORD)
    public void test00CreateIndex() {
        // Checks if the index has been created
        IndicesExistsResponse existResponse = adminClient.indices()
                .prepareExists(RIVER_KEYWORD)
                .execute().actionGet();

        Assert.assertTrue("Index must exist", existResponse.isExists());
    }

    @Test
    @ElasticsearchIndex(indexName = RIVER_KEYWORD)
    public void test10IndexingFromRevision() throws IOException {
        // Index a new document
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("repos",REPOS);
        json.put("path",PATH);
        json.put("start_revision",START_REVISION);

        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("type", "svn")
                .field("svn",json)
                .endObject();

        IndexResponse indexResponse = client1.prepareIndex(RIVER_KEYWORD, "mysvnriver", "_meta")
                .setSource(builder)
                .execute()
                .actionGet();

        Assert.assertTrue("Indexing must return a version >= 1",
                indexResponse.getVersion() >= 1);
    }

    @Test
    @ElasticsearchIndex(indexName = RIVER_KEYWORD)
    public void test11IndexingLastRevision() throws IOException {
        // Index a new document
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("repos",REPOS);
        json.put("path",PATH);

        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("type", "svn")
                .field("svn",json)
                .endObject();

        IndexResponse indexResponse = client1.prepareIndex(RIVER_KEYWORD, "mysvnriver2", "_meta")
                .setSource(builder)
                .execute()
                .actionGet();

        Assert.assertTrue("Indexing must return a version >= 1",
                indexResponse.getVersion() >= 1);
    }

    @Test
    @ElasticsearchIndex(indexName = RIVER_KEYWORD)
    public void test20Searching() {
        // Wait 2s for the indexing to take place.
        try {
            sleep(3000L);
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }

        SearchResponse searchResponse = client1.prepareSearch("mysvnriver")
                .setQuery(QueryBuilders.matchPhrasePrefixQuery("name", "watchlist"))
                .execute()
                .actionGet();

        for(SearchHit hit : searchResponse.getHits()) {
            System.out.println("Search result index ["+hit.index()
                    +"] type ["+hit.type()
                    +"] id ["+hit.id()+"]"
            );
            System.out.println("Search result source:"+hit.sourceAsString());
        }

        Assert.assertTrue("There should be a watchlist.txt in the repository",
                searchResponse.getHits().totalHits() > 0);
    }

    @Test
    @ElasticsearchIndex(indexName = RIVER_KEYWORD)
    public void test21GetIndexedRevision() {
        // Wait 2s for the indexing to take place.
        try {
            sleep(4000L);
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }

        System.out.println("Preparing to get Indexed Revision on index [mysvnriver]" +
                " with id ["+INDEXED_REVISION_ID+"]");
        GetResponse response = client1.prepareGet("mysvnriver", "indexed_revision", INDEXED_REVISION_ID)
                .setFields("revision")
                .execute()
                .actionGet();
        Long result = (Long) response.getField("revision").getValue();
        System.out.println("Get Indexed Revision Response index ["+response.getIndex()
                +"] type ["+response.getType()
                +"] id ["+response.getId()
                +"] value ["+result+"]");

        Assert.assertTrue("Indexed Revision must be a number > 0",
                result > 0L);
    }

    @Test
    @ElasticsearchIndex(indexName = RIVER_KEYWORD)
    public void test22GetMapping() throws IOException {
        // Wait 2s for the indexing to take place.
        try {
            sleep(4000L);
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }

        ClusterState cs = client1.admin().cluster().prepareState()
                .setFilterIndices("mysvnriver")
                .execute().actionGet().getState();
        IndexMetaData imd = cs.getMetaData().index("mysvnriver");
        MappingMetaData mdd = imd.mapping(SubversionDocument.TYPE_NAME);

        System.out.println("Get Mapping"
                +" type ["+mdd.type()
                +"] id ["+mdd.id()
                +"] source ["+mdd.source()+"]"
        );
        boolean found = mdd.source().string().contains("not_analyzed");

        Assert.assertTrue("Mapping for SubversionDocument must contain not_analyzed", found);
    }

}
