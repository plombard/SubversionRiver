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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.net.URI;
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
            startRevision = XContentMapValues.nodeLongValue(subversionSettings.get("start_revision"), INDEX_HEAD_REVISION);
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
        if(!existResponse.exists()) {
            logger.info("Get Indexed Revision Index [{}] does not exists : {}",
                    indexName,existResponse.exists());
            return NOT_INDEXED_REVISION;
        }
        GetResponse response = client.prepareGet("_river", indexName, indexedRevisionID)
                .setFields("indexed_revision")
                .execute()
                .actionGet();
        logger.info("Get Indexed Revision Index [{}] Type [{}] Id [{}] Fields [{}]",
                "_river", indexName, indexedRevisionID, response.fields());

        if(response.field("indexed_revision") == null) {
            return NOT_INDEXED_REVISION;
        } else {
            return (long) (Integer) response.field("indexed_revision").value();
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
                    logger.info("Indexed Revision Value [{}]",indexedRevision);
                    List<BulkRequestBuilder> bulks = Lists.newArrayList();

                    long lastRevision = SubversionCrawler.getLatestRevision(reposAsFile, path);
                    logger.info("Checking last revision of repository : {}/{} --> [{}]",
                            reposAsFile, path, lastRevision);

                    // if indexed revision is the last revision, we have nothing to do
                    // but if it's not, we index the new subversion updates.
                    if(indexedRevision < lastRevision) {
                        UpdatePolicy updatePolicy = getUpdatePolicy(lastRevision);

                        logger.info("Indexing repository {}/{} from revision [{}] --> [{}] incremental [{}]",
                                reposAsFile, path, updatePolicy.fromRevision, lastRevision, updatePolicy.incremental);

                        for(Long revision = updatePolicy.fromRevision;revision<=updatePolicy.toRevision;revision++) {
                            logger.info("Now indexing repository {}/{} revision [{}]",
                                    reposAsFile, path, revision);

                            // TODO : see wether it's worth the hassle.
                            // For data consistency, we delete every reference to documents
                            // in that path, as they'll be parsed and added next.
                            // If we don't, we'd have to deal with deleted files still referenced in the index.

                            // The total list of subversion documents is partitioned
                            // into smaller lists, of max size bulksize
                            List<List<SubversionDocument>> subversionDocuments =
                                    Lists.partition(
                                            SubversionCrawler.SvnList(reposAsFile, path, lastRevision),
                                            bulkSize);
                            // Index only the documents with a doc.revision >= revision,
                            // as they are the only ones modified since last pass,
                            // unless we are in the initial indexing pass.
                            for( List<SubversionDocument> subversionDocumentList:subversionDocuments) {
                                BulkRequestBuilder bulk = client.prepareBulk();
                                for( SubversionDocument svnDocument:subversionDocumentList ) {
                                    if( !updatePolicy.incremental
                                            || svnDocument.revision >= revision) {
                                        bulk.add(indexRequest(indexName)
                                                .type(typeName)
                                                .id(svnDocument.id())
                                                .source(svnDocument.json()));
                                        logger.debug("Document added to queue :{}",svnDocument.json());
                                    }
                                }
                                bulks.add(bulk);
                                totalNumberOfActions += bulk.numberOfActions();
                            }
                        }
                    }

                    if(totalNumberOfActions > 0) {
                        indexedRevision = lastRevision;
                        executeBulks(bulks);
                    } else {
                        logger.info("Nothing to index (latest revision reached ? [{}]) in {}/{}",
                                indexedRevision, reposAsFile, path);
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
                    logger.warn("failed to execute" + response.buildFailureMessage());
                }
            } catch (Exception e) {
                logger.warn("failed to execute bulk", e);
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
     * @return an UpdatePolicy with the start, end revisions, and incremental behavior
     */
    private UpdatePolicy getUpdatePolicy(Long lastRevision) {
        // If repository has not been indexed yet
        // we start from the revision specified
        // in the settings, possibly last one (HEAD).
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

        return result;
    }
}
