package org.elasticsearch.river.subversion;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.subversion.type.SubversionDocument;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.currentThread;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;

@ClusterScope(scope=Scope.SUITE, numNodes=1)
public class SubversionRiverTest extends ElasticsearchIntegrationTest {

    protected ESLogger logger = ESLoggerFactory.getLogger(SubversionRiverTest.class.getName());

    private static final HashFunction hf = Hashing.md5();
    private static final String PATH = "/";
    private static final Long START_REVISION = 4L;
    private static String REPOS;
    private static String INDEXED_REVISION_ID;

    @SuppressWarnings("ConstantConditions")
    @Before
    public void setUp() throws Exception {
        super.setUp();
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
    public void testSubversionRiver() throws IOException {
        logger.info("-- Lauching Tests --");
        // Index a new river metadata to create a river
        XContentBuilder builder = jsonBuilder()
            .startObject()
                .field("type", "svn")
                .startObject("svn")
                    .field("repos", REPOS)
                    .field("path", PATH)
                    .field("start_revision", START_REVISION)
                .endObject()
            .endObject();

        logger.info("-- Creating first river (with start revision) --");
        IndexResponse indexResponse = client().prepareIndex("_river", "mysvnriver", "_meta")
                .setSource(builder)
                .execute()
                .actionGet();

        // Wait for the cluster availability
        client().admin().cluster().prepareHealth()
            .setWaitForYellowStatus()
            .execute().actionGet();

        Assert.assertTrue("Indexing must return a version >= 1",
                indexResponse.getVersion() >= 1);
        logger.info("-- First river OK --");

        // Create a second river, without start revision
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("repos",REPOS);
        json.put("path",PATH);

        builder = jsonBuilder()
                .startObject()
                .field("type", "svn")
                .field("svn",json)
                .endObject();

        // Wait for the cluster availability
        client().admin().cluster().prepareHealth()
            .setWaitForYellowStatus()
            .execute().actionGet();

        logger.info("-- Creating second river (without start revision) --");
        indexResponse = client().prepareIndex("_river", "mysvnriver2", "_meta")
                .setSource(builder)
                .execute()
                .actionGet();

        Assert.assertTrue("Indexing must return a version >= 1",
                indexResponse.getVersion() >= 1);
        logger.info("-- Second river OK --");

        // Wait 3s for the indexing to take place.
        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }

        logger.info("-- Testing search --");
        SearchResponse searchResponse = client().prepareSearch("mysvnriver")
                .setQuery(QueryBuilders.matchPhrasePrefixQuery("name", "watchlist"))
                .execute()
                .actionGet();

        for(SearchHit hit : searchResponse.getHits()) {
            logger.info("Search result index [{}] type [{}] id [{}]",
                    hit.index(), hit.type(), hit.id()
            );
            logger.info("Search result source: [{}]",hit.sourceAsString());
        }

        Assert.assertTrue("There should be a watchlist.txt in the repository",
                searchResponse.getHits().totalHits() == 3);
        logger.info("-- Search OK --");

        logger.info("-- Testing getIndexedRevision --");
        logger.info("Preparing to get Indexed Revision on index [mysvnriver] with id [{}]",
                INDEXED_REVISION_ID);
        GetResponse response = client().prepareGet("mysvnriver", "indexed_revision",
                INDEXED_REVISION_ID)
                .setFields("revision")
                .execute()
                .actionGet();
        Long result = (Long) response.getField("revision").getValue();
        logger.info("Get Indexed Revision Response index [{}] type [{}] id [{}] value [{}]",
                response.getIndex(), response.getType(), response.getId(), result);

        Assert.assertTrue("Indexed Revision must be 8",
                result == 8L);
        logger.info("-- getIndexedRevision OK --");

        logger.info("-- Testing getMapping --");
        ClusterState cs = client().admin().cluster().prepareState()
                .setIndices("mysvnriver")
                .execute().actionGet().getState();
        IndexMetaData imd = cs.getMetaData().index("mysvnriver");
        MappingMetaData mdd = imd.mapping(SubversionDocument.TYPE_NAME);

        logger.info("Get Mapping type [{}] id [{}] source [{}]",
                mdd.type(), mdd.id(), mdd.source()
        );
        boolean found = mdd.source().string().contains("not_analyzed");

        Assert.assertTrue("Mapping for SubversionDocument must contain not_analyzed", found);
        logger.info("-- getMapping OK --");
    }

}
