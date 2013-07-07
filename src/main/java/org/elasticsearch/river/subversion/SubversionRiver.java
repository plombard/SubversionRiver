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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
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
import org.elasticsearch.river.subversion.beans.SubversionRevision;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;

/**
 * River for SVN repositories
 */
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
    private static final Long NOT_INDEXED_REVISION = 0L;
    private static final Long INDEX_HEAD_REVISION = -1L;

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
            startRevision = XContentMapValues.nodeLongValue(subversionSettings.get("start_revision"), 1L);
        }

        indexedRevisionID ="_indexed_revision_".concat(
                hf.newHasher()
                        .putString(repos)
                        .putString(path)
                        .hash()
                        .toString()
        );

    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Override
    public void start() {
        logger.info("Starting Subversion River: repos [{}], path [{}], updateRate [{}], bulksize [{}], " +
                "startRevision [{}], indexing to [{}]/[{}]",
                repos, path, updateRate, bulkSize, startRevision, indexName, typeName);
        try {
            client.admin().indices()
                    .prepareCreate(indexName)
                    .execute().actionGet();
            client.admin().indices()
                    .preparePutMapping(indexName)
                    .setType("svn")
                    .setSource(SubversionMapping.getInstance())
                    .execute().actionGet();
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof IndexAlreadyExistsException) {
                // that's fine
            } else if (cause instanceof ClusterBlockException) {
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
     * return 0 if the field does not exist (yet)
     * or if the index has not been created (yet).
     * @return last indexed revision
     */
    private Long getIndexedRevision() {
        // Checks if the index has been created
        IndicesExistsResponse existResponse = client.admin().indices()
                .prepareExists(indexName)
                .execute().actionGet();
        // If the index does not exist
        // return 0
        if(!existResponse.isExists()) {
            logger.info("Get Indexed Revision Index [{}] does not exists : {}",
                    indexName,existResponse.isExists());
            return NOT_INDEXED_REVISION;
        }
        GetResponse response = client.prepareGet("_river", indexName, indexedRevisionID)
                .setFields("indexed_revision")
                .execute()
                .actionGet();
        logger.info("Get Indexed Revision Index [{}] Type [{}] Id [{}] Fields [{}]",
                "_river", indexName, indexedRevisionID, response.getFields());

        if(response.getField("indexed_revision") == null
                || !response.isExists()) {
            return NOT_INDEXED_REVISION;
        } else {
            return (Long) response.getField("indexed_revision").getValue();
        }
    }

    /**
     * Main Indexer Class
     */
    private class Indexer implements Runnable {

        @Override
        public void run() {
            while (true) {
                if (closed) {
                    return;
                }

                try {
                    int totalNumberOfActions = 0;
                    logger.info("Indexing subversion repository : {}/{}", repos, path);
                    File reposAsFile = new File(new URI(repos));

                    indexedRevision = getIndexedRevision();
                    logger.info("Indexed Revision Value [{}]", indexedRevision);
                    List<BulkRequestBuilder> bulks = Lists.newArrayList();

                    long lastRevision = SubversionCrawler.getLatestRevision(reposAsFile, path);
                    logger.info("Checking last revision of repository : {}/{} --> [{}]",
                            reposAsFile, path, lastRevision);

                    // if indexed revision is the last revision, we have nothing to do
                    // but if it's not, we index the new subversion updates.
                    if (indexedRevision < lastRevision) {
                        UpdatePolicy updatePolicy = getUpdatePolicy(lastRevision, bulkSize);

                        logger.info("Indexing repository {}/{} from revision [{}] to [{}] incremental [{}]",
                                reposAsFile, path, updatePolicy.fromRevision, updatePolicy.toRevision, updatePolicy.incremental);

                        logger.info("Now indexing repository {}/{} revision [{}] to [{}]",
                                reposAsFile, path, updatePolicy.fromRevision, updatePolicy.toRevision);

                        // The total list of subversion documents is partitioned
                        // into smaller lists, of max size bulksize
                        List<SubversionRevision> subversionRevisionsBulk =
                                SubversionCrawler.getRevisions(
                                        reposAsFile,
                                        path,
                                        Optional.of(updatePolicy.fromRevision),
                                        Optional.of(updatePolicy.toRevision)
                                );
                        // Send the revisions in bulk to the index
                        BulkRequestBuilder bulk = client.prepareBulk();
                        for (SubversionRevision svnRevision : subversionRevisionsBulk) {
                            bulk.add(indexRequest(indexName)
                                    .type(typeName)
                                    .id(svnRevision.id())
                                    .source(svnRevision.json()));
                            logger.debug("Document added to queue :{}", svnRevision.json());
                        }
                        bulks.add(bulk);
                        totalNumberOfActions += bulk.numberOfActions();
                        executeBulksAndSetLastRevision(totalNumberOfActions,
                                bulks,
                                updatePolicy.toRevision);
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

    /**
     *  Execute indexing actions if necessary
     * @param totalNumberOfActions number of actions projected
     * @param bulks list of actions to execute
     * @param lastRevision last revision of the repos
     */
    private void executeBulksAndSetLastRevision(int totalNumberOfActions,
                                                List<BulkRequestBuilder> bulks,
                                                long lastRevision) {
        if(totalNumberOfActions > 0) {
            indexedRevision = lastRevision;
            executeBulks(bulks);
        } else {
            logger.info("Nothing to index (latest revision reached ? [{}]) in {}/{}",
                    indexedRevision, repos, path);
        }
    }

    /**
     * Execute a List of Bulks
     * @param bulks a bunch of bulks
     */
    private void executeBulks(List<BulkRequestBuilder> bulks) {
        for( BulkRequestBuilder bulk : bulks)  {
            // Update the last indexed revision
            bulk.add(indexRequest("_river")
                    .type(indexName)
                    .id(indexedRevisionID)
                    .source("indexed_revision", indexedRevision)
            );
            logger.info("Indexed revision of repository : {} --> [{}]", path, indexedRevision);

            try {
                logger.info("Execute bulk {} actions", bulk.numberOfActions());
                BulkResponse response = bulk.execute().actionGet();
                if (response.hasFailures()) {
                    logger.error("failed to execute" + response.buildFailureMessage());
                }
            } catch (Exception e) {
                logger.error("failed to execute bulk", e);
            }
        }
    }


    /**
     * POJO for the river update behavior
     */
    private class UpdatePolicy {
        Long fromRevision;
        Long toRevision;
        Boolean incremental; // Are we indexing from scratch ?
    }

    /**
     * Based on the last revision and the river parameters,
     * return what should be the update behavior of the river
     * @param lastRevision last revision of the repository to index
     * @param bulkSize the size of the document batch to index simultaneously
     * @return an UpdatePolicy with the start, end revisions, and incremental behavior
     */
    private UpdatePolicy getUpdatePolicy(Long lastRevision, Integer bulkSize) {
        // If repository has not been indexed yet
        // we start from the revision specified
        // in the settings. (default is revision 1)
        UpdatePolicy result = new UpdatePolicy();

        if( indexedRevision == NOT_INDEXED_REVISION) {
            result.incremental = false;
            if( startRevision == INDEX_HEAD_REVISION) {
                result.fromRevision = lastRevision;
                result.toRevision = lastRevision;
            } else {
                result.fromRevision = startRevision;
                result.toRevision = lastRevision;
            }
        } else {
            result.incremental = true;
            if(indexedRevision+1L < lastRevision) {
                result.fromRevision = indexedRevision+1L;
            } else {
                result.fromRevision = lastRevision;
            }
            result.toRevision = lastRevision;
        }
        // handling batch size
        if( result.fromRevision+bulkSize < result.toRevision ) {
            result.toRevision = result.fromRevision+bulkSize;
        }

        return result;
    }
}
