package org.elasticsearch.river.subversion;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.*;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;

/**
 * River for SVN repositories
 */
public class SubversionRiver extends AbstractRiverComponent implements River {

    private Client client;
    private ThreadPool threadPool;

    private String riverIndexName;
    private String indexName = null;
    private String typeName = null;
    private String repos;
    private String path;
    private int updateRate;

    private volatile boolean closed;
    private volatile Thread indexerThread;

    @Inject
    protected SubversionRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName,
                              Client client, ThreadPool threadPool) {
        super(riverName, settings);
        logger.info("Creating subversion river");
        this.client = client;
        this.threadPool = threadPool;
        this.riverIndexName = riverIndexName;
        if (settings.settings().containsKey("subversion")) {
            Map<String, Object> subversionSettings = (Map<String, Object>) settings.settings().get("subversion");

            repos = XContentMapValues.nodeStringValue(subversionSettings.get("repos"), null);
            path = XContentMapValues.nodeStringValue(subversionSettings.get("path"), "/");
            updateRate = XContentMapValues.nodeIntegerValue(subversionSettings.get("update_rate"), 15 * 60 * 1000);

            indexName = riverName.name();
            typeName = "page";
        }
    }

    @Override
    public void start() {
        logger.info("Starting Subversion River: repos [{}], path [{}], updateRate [{}], indexing to [{}]/[{}]",
                repos, path, updateRate, indexName, typeName);
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

        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "subversion_river_indexer").newThread(new Indexer());
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

    private class Indexer implements Runnable {

        // TODO: implement bulk size
        // TODO: implement basic mapping
        // TODO: stop it from looping (implement diff updates)
        @Override
        public void run() {
            while (true) {
                if (closed) {
                    return;
                }

                try {
                    logger.info("Indexing subversion repository : {}/{}", repos, path);
                    BulkRequestBuilder bulk = client.prepareBulk();

                    List<SVNDocument> result = Browser.SvnList(repos,path);
                    for( SVNDocument svnDocument:result ) {
                        logger.info("Document added to queue :{}",svnDocument.toJson());
                        bulk.add(indexRequest(indexName)
                                .type(typeName)
                                .id(Integer.toString(svnDocument.hashCode()))
                                .source(svnDocument.toJson()));
                    }

                    try {
                        logger.info("Execute bulk {} actions", bulk.numberOfActions());
                        BulkResponse response = bulk.execute().actionGet();
                        if (response.hasFailures()) {
                            logger.warn("failed to execute" + response.buildFailureMessage());
                        }
                    } catch (Exception e) {
                        logger.warn("failed to execute bulk", e);
                    }
                } catch (Exception e) {
                    logger.warn("Subversion river exception", e);
                }

                try {
                    logger.debug("Subversion river is going to sleep for {} ms", updateRate);
                    Thread.sleep(updateRate);
                } catch (InterruptedException e) {
                }

            }

        }
    }
}
