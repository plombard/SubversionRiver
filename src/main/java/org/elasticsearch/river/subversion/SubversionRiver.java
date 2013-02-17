package org.elasticsearch.river.subversion;

import com.google.common.base.Objects;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;

/**
 * River for SVN repositories
 */
// TODO : implement integration tests
public class SubversionRiver extends AbstractRiverComponent implements River {

    private Client client;
    private ThreadPool threadPool;

    private String indexName = null;
    private String typeName = null;
    private String repos;
    private String path;
    private int updateRate;
    private int bulkSize;
    private long indexedRevision;
    private long startRevision;
    private String indexedRevisionID;

    private volatile boolean closed;
    private volatile Thread indexerThread;

    private static final HashFunction hf = Hashing.md5();

    @Inject
    protected SubversionRiver(RiverName riverName,
                              RiverSettings settings,
                              Client client,
                              ThreadPool threadPool) {
        super(riverName, settings);
        logger.info("Creating subversion river");
        this.client = client;
        this.threadPool = threadPool;
        if (settings.settings().containsKey("svn")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> subversionSettings = (Map<String, Object>) settings.settings().get("svn");

            repos = XContentMapValues.nodeStringValue(subversionSettings.get("repos"), null);
            path = XContentMapValues.nodeStringValue(subversionSettings.get("path"), "/");
            updateRate = XContentMapValues.nodeIntegerValue(subversionSettings.get("update_rate"), 15 * 60 * 1000);
            indexName = riverName.name();
            typeName = XContentMapValues.nodeStringValue(subversionSettings.get("type"), "svn");
            bulkSize = XContentMapValues.nodeIntegerValue(subversionSettings.get("bulk_size"), 200);
            startRevision = XContentMapValues.nodeLongValue(subversionSettings.get("start_revision"), -1L);
        }

        indexedRevisionID ="_indexed_revision_".concat(
                hf.newHasher()
                        .putString(repos)
                        .putString(path)
                        .hash()
                        .toString()
        );

    }

    @Override
    public void start() {
        logger.info("Starting Subversion River: repos [{}], path [{}], updateRate [{}], bulksize [{}], " +
                "startRevision [{}], indexing to [{}]/[{}]",
                repos, path, updateRate, bulkSize, startRevision, indexName, typeName);
        try {
            client.admin().indices().prepareCreate(indexName).execute().actionGet();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "subversion_river_indexer")
                .newThread(new Indexer());
        indexerThread.start();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("Stopping Subversion River");
        indexerThread.interrupt();
        closed = true;
    }

    /**
     * Gives the last indexed revision of the repository path
     * return -1 if the field does not exist (yet)
     * return 0 if the index has not been created (yet).
     * @return last indexed revision
     */
    private Long getIndexedRevision() {
        if( indexedRevision == 0L) {
            return 0L;
        }
        // Checks if the index has been created
        IndicesExistsResponse existResponse = client.admin().indices()
                .prepareExists(indexName)
                .execute().actionGet();
        // If the index does not exist
        // return 0
        if(!existResponse.exists()) {
            return 0L;
        }
        GetResponse response = client.prepareGet(indexName, typeName, indexedRevisionID)
                .execute()
                .actionGet();
        logger.debug("Get Indexed Revision Response :"+response.sourceAsString());

        return (Long) Objects.firstNonNull(response.field("indexed_revision"),-1L);
    }

    /**
     * Main Indexer Class
     */
    private class Indexer implements Runnable {

        // TODO: implement bulk size
        // TODO: implement diff updates
        @Override
        public void run() {
            while (true) {
                if (closed) {
                    return;
                }

                try {

                    logger.info("Indexing subversion repository : {}/{}", repos, path);
                    File reposAsFile = new File(Thread.currentThread()
                            .getContextClassLoader()
                            .getResource(repos)
                            .toURI()
                    );

                    indexedRevision = getIndexedRevision();

                    BulkRequestBuilder bulk = client.prepareBulk();

                    long lastRevision = SubversionCrawler.getLatestRevision(reposAsFile, path);
                    logger.info("Checking last revision of repository : {}/{} --> [{}]", reposAsFile, path, lastRevision);
                    // If lastRevision is strictly superior to indexedRevision,
                    // there have been updates to the repository, so we index them
                    if( lastRevision > indexedRevision) {

                        // For data consistency, we delete every reference to documents
                        // in that path, as they'll be parsed and added next.
                        // If we don't, we'd have to deal with deleted files still referenced in the index.

                        List<SubversionDocument> result =
                                SubversionCrawler.SvnList(reposAsFile, path, lastRevision);

                        for( SubversionDocument svnDocument:result ) {
                            bulk.add(indexRequest(indexName)
                                    .type(typeName)
                                    .id(svnDocument.id())
                                    .source(svnDocument.json()));
                            logger.debug("Document added to queue :{}",svnDocument.json());
                        }

                        // Update the last indexed revision
                        indexedRevision = lastRevision;
                        bulk.add(indexRequest("_river")
                                .type(indexName)
                                .id(indexedRevisionID)
                                .source("indexed_revision",indexedRevision)
                        );
                        logger.info("Indexed revision of repository : {}/{} --> [{}]", reposAsFile, path, indexedRevision);
                    } else {
                        logger.info("Nothing to index (latest revision reached ? [{}]) in {}/{}",
                                indexedRevision, reposAsFile, path);
                    }

                    if(bulk.numberOfActions() > 0) {
                        try {
                            logger.info("Execute bulk {} actions", bulk.numberOfActions());
                            BulkResponse response = bulk.execute().actionGet();
                            if (response.hasFailures()) {
                                logger.warn("failed to execute" + response.buildFailureMessage());
                            }
                        } catch (Exception e) {
                            logger.warn("failed to execute bulk", e);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Subversion river exception", e);
                }

                try {
                    logger.debug("Subversion river is going to sleep for {} ms", updateRate);
                    Thread.sleep(updateRate);
                } catch (InterruptedException e) {
                    // we shamefully swallow the interrupted exception
                    logger.warn("Subversion river interrupted");
                }

            }

        }
    }
}
