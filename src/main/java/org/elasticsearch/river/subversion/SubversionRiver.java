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
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.subversion.mapping.IndexedRevisionMapping;
import org.elasticsearch.river.subversion.mapping.SubversionDocumentMapping;
import org.elasticsearch.river.subversion.mapping.SubversionRevisionMapping;
import org.elasticsearch.river.subversion.type.SubversionDocument;
import org.elasticsearch.river.subversion.type.SubversionRevision;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * River for SVN repositories
 */
public class SubversionRiver extends AbstractRiverComponent implements River {

    // Parameters
    private String indexName = null;
    private String typeName = null;
    private String repos;
    private String login;
    private String password;
    private String path;
    private int updateRate;
    private int bulkSize;
    private int bulkConcurrentRequests;
    private long indexedRevision;
    private long startRevision;
    private String indexedRevisionID;

    // Core utility classes
    private Client client;
    private BulkProcessor bulkProcessor;
    private volatile boolean closed;
    private volatile Thread indexerThread;
    @SuppressWarnings("unused")
    private final ThreadPool threadPool;

    // Static variables
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
            login = XContentMapValues.nodeStringValue(subversionSettings.get("login"), "anonymous");
            password = XContentMapValues.nodeStringValue(subversionSettings.get("password"), "password");
            path = XContentMapValues.nodeStringValue(subversionSettings.get("path"), "/");
            updateRate = XContentMapValues.nodeIntegerValue(subversionSettings.get("update_rate"), 15 * 60 * 1000);
            indexName = XContentMapValues.nodeStringValue(subversionSettings.get("index"), riverName.name());
            typeName = XContentMapValues.nodeStringValue(subversionSettings.get("type"), "svn");
            bulkSize = XContentMapValues.nodeIntegerValue(subversionSettings.get("bulk_size"), 200);
            bulkConcurrentRequests = XContentMapValues.nodeIntegerValue(subversionSettings.get("bulk_concurrent_requests"), 1);
            startRevision = XContentMapValues.nodeLongValue(subversionSettings.get("start_revision"), 1L);
        }

        bulkProcessor = BulkProcessor.builder(client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                        // Add the last revision number to index to the bulk
                        // So it can be retrieved from the ElasticSearch index.
                        try {
                            bulkRequest.add(indexRequest(indexName)
                                    .type("indexed_revision")
                                    .id(indexedRevisionID)
                                    .source(
                                            jsonBuilder()
                                                    .startObject()
                                                        .field("repos", repos)
                                                        .field("revision", indexedRevision)
                                                    .endObject()
                                    )
                            );
                            logger.info("Updating indexed_revision on index [{}] with id [{}] and value {repos:[{}],revision:[{}]}",
                                indexName, indexedRevisionID, repos, indexedRevision);
                        } catch (IOException ioe) {
                            logger.error("failed to update indexed_revision [{}] on index [{}]" +
                                    " because of Exception {}, it will be set with the next bulk operation",
                                    indexedRevision, indexName, ioe
                            );
                        }
                        logger.info("Indexed revision of repository : {}{} --> [{}]", repos, path, indexedRevision);
                        logger.info("Execute bulk {} actions", bulkRequest.numberOfActions());

                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkItemResponses) {
                        logger.info("Completed bulk {} actions in {}ms",
                                bulkRequest.numberOfActions(),
                                bulkItemResponses.getTookInMillis());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
                        logger.error("Failed to execute bulk, exception ",throwable);
                    }
                }
        )
                .setBulkActions(bulkSize)
                .setConcurrentRequests(bulkConcurrentRequests)
                .setFlushInterval(TimeValue.timeValueMillis(updateRate))
        .build();

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
            // Wait for the cluster availability
            client.admin().cluster().prepareHealth()
                    .setWaitForYellowStatus()
                    .execute().actionGet();
            // Checks if the index has already been created
            IndicesExistsResponse existResponse = client.admin().indices()
                    .prepareExists(indexName)
                    .execute().actionGet();
            if(!existResponse.isExists()) {
                logger.info("Subversion River: Index [{}] does not exists, creating...",
                        indexName);
                client.admin().indices()
                        .prepareCreate(indexName)
                        .execute().actionGet();
                // Wait for the cluster availability
                client.admin().cluster().prepareHealth()
                        .setWaitForYellowStatus()
                        .execute().actionGet();
            }
            // Create Mappings if needed
            CreateMapping(SubversionRevision.TYPE_NAME, SubversionRevisionMapping.getInstance());
            CreateMapping(SubversionDocument.TYPE_NAME, SubversionDocumentMapping.getInstance());
            CreateMapping("indexed_revision", IndexedRevisionMapping.getInstance());
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (!(cause instanceof IndexAlreadyExistsException) && !(cause instanceof ClusterBlockException)) {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        indexerThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "subversion_river_indexer")
                .newThread(new Indexer());
        indexerThread.start();
    }

    /**
     * Create mapping for specified type if it does not exists.
     * @param type elasticsearch type name
     * @param mapping elasticsearch mapping instance
     * @throws IOException
     */
    private void CreateMapping(String type, XContentBuilder mapping) throws IOException {
        GetResponse getResponse = client.prepareGet(indexName, type, "_mapping")
                .execute()
                .actionGet();
        if( !getResponse.isExists() ) {
            logger.info("Subversion River: Mapping for type [{}] does not exists, creating...",
                    type);
            client.admin().indices()
                    .preparePutMapping(indexName)
                    .setType(type)
                    .setSource(mapping)
                    .execute().actionGet();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("Stopping Subversion River");
        bulkProcessor.close();
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
        // Wait for the index availability
        client.admin().cluster().prepareHealth()
                .setWaitForYellowStatus()
                .execute().actionGet();
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

        // Attempt to get the last indexed revision with a GET.
        // A NullPointerException basically means that there is no indexed_revision.
        GetResponse response = client.prepareGet(indexName, "indexed_revision", indexedRevisionID)
                .setFields("revision")
                .execute()
                .actionGet();
        logger.debug("Get Indexed Revision Index [{}] Type [{}] Id [{}] Fields [{}]",
                indexName, "indexed_revision", indexedRevisionID, response.getFields());

        Optional<GetField> indexedRevisionField = Optional.fromNullable(response.getField("revision"));
        if( !indexedRevisionField.isPresent()
                || !response.isExists()) {
            logger.info("Problem encountered while GETting indexed_revision on [{}] (does not exist ?).",
                    indexName);
            return NOT_INDEXED_REVISION;
        } else {
            return (Long) indexedRevisionField.get().getValue();
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
                    logger.info("Indexing subversion repository : {}/{}", repos, path);
                    URL reposAsURL = new URL(repos);

                    indexedRevision = getIndexedRevision();
                    logger.info("Indexed Revision Value [{}]", indexedRevision);

                    long lastRevision = SubversionCrawler.getLatestRevision(reposAsURL, login, password, path);
                    logger.debug("Checking last revision of repository : {}/{} --> [{}]",
                            reposAsURL, path, lastRevision);

                    // if indexed revision is the last revision, we have nothing to do
                    // but if it's not, we index the new subversion updates.
                    if (indexedRevision < lastRevision) {
                        UpdatePolicy updatePolicy = getUpdatePolicy(lastRevision);

                        logger.debug("Indexing repository {}/{} from revision [{}] to [{}] incremental [{}]",
                                reposAsURL, path, updatePolicy.fromRevision, updatePolicy.toRevision, updatePolicy.incremental);

                        // Optimistic update to the last revision indexed
                        // At this point, either the retrieving from the repository
                        // will fail, and the indexed revision will be reset
                        // from the index on the next pass,
                        // Or... all will go well (finger-crossed).
                        indexedRevision = updatePolicy.toRevision;

                        // Retrieve the revisions from the repository
                        List<SubversionRevision> subversionRevisionsBulk =
                                SubversionCrawler.getRevisions(
                                        reposAsURL,
                                        login,
                                        password,
                                        path,
                                        Optional.of(updatePolicy.fromRevision),
                                        Optional.of(updatePolicy.toRevision)
                                );
                        // Send the revisions in bulk to the index
                        for (SubversionRevision svnRevision : subversionRevisionsBulk) {
                            // First the revision...
                            bulkProcessor.add(indexRequest(indexName)
                                    .type(SubversionRevision.TYPE_NAME)
                                    .id(svnRevision.id())
                                    .source(svnRevision.json())
                            );
                            // ... and then the documents/files
                            for (SubversionDocument svnDocument : svnRevision.getDocuments()) {
                                bulkProcessor.add(indexRequest(indexName)
                                        .type(SubversionDocument.TYPE_NAME)
                                        .source(svnDocument.json())
                                );
                            }
                            logger.debug("Revision added to queue :{}", svnRevision.json());
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
        // Or if the last indexed revision is inferior
        // to the one in the settings
        // we start from the revision specified
        // in the settings. (default is revision 1)
        UpdatePolicy result = new UpdatePolicy();

        if( indexedRevision == NOT_INDEXED_REVISION
                || indexedRevision < startRevision ) {
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
