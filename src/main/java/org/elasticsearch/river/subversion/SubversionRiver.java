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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.subversion.crawler.Parameters;
import org.elasticsearch.river.subversion.crawler.SubversionCrawler;
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

    private Client client;
    @SuppressWarnings("unused")
    private ThreadPool threadPool;

    private String indexName = null;
    private String typeName = null;
    private String repos;
    private Parameters crawlerParameters;
    private int updateRate;
    private int bulkSize;
    private long indexedRevision;
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

            // Crawler settings
            repos = XContentMapValues.nodeStringValue(subversionSettings.get("repos"), null);
            crawlerParameters = new Parameters.ParametersBuilder()
                .setLogin(XContentMapValues.nodeStringValue(
                    subversionSettings.get("login"), null))
                .setPassword(XContentMapValues.nodeStringValue(
                    subversionSettings.get("password"), null))
                .setPath(XContentMapValues.nodeStringValue(
                    subversionSettings.get("path"), null))
                // Really NOT happy AT ALL to have to define default values
                // both here *and* in the Parameters class
                // because of an implicit cast to integer
                // in settings.
                .setStartRevision(XContentMapValues.nodeLongValue(
                    subversionSettings.get("start_revision"), 1L))
                .setEndRevision(XContentMapValues.nodeLongValue(
                    subversionSettings.get("end_revision"), 0L))
                .setMaximumFileSize(XContentMapValues.nodeLongValue(
                        subversionSettings.get("maximum_file_size"), 0L))
                //.setPatternsToFilter((Set<Pattern>) subversionSettings.get("patterns_to_filter"))
                .setStoreDiffs(XContentMapValues.nodeBooleanValue(
                    subversionSettings.get("store_diffs"), false))
            .create();
            logger.info("Init Subversion river, crawler parameters [{}]", crawlerParameters);
            // River settings
            updateRate = XContentMapValues.nodeIntegerValue(subversionSettings.get("update_rate"), 15 * 60 * 1000);
            indexName = XContentMapValues.nodeStringValue(subversionSettings.get("index"), riverName.name());
            typeName = XContentMapValues.nodeStringValue(subversionSettings.get("type"), "svn");
            bulkSize = XContentMapValues.nodeIntegerValue(subversionSettings.get("bulk_size"), 200);
        }

        indexedRevisionID ="_indexed_revision_".concat(
                hf.newHasher()
                        .putUnencodedChars(repos)
                        .putUnencodedChars(crawlerParameters.getPath().get())
                        .hash()
                        .toString()
        );

    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Override
    public void start() {
        logger.info("Starting Subversion River: repos [{}], path [{}], updateRate [{}], bulksize [{}], " +
                "startRevision [{}], indexing to [{}]/[{}]",
                repos, crawlerParameters.getPath().get(),
                updateRate, bulkSize, crawlerParameters.getStartRevision().get(),
                indexName, typeName);
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
                    int totalNumberOfActions = 0;
                    logger.info("Indexing subversion repository : {}/{}", repos, crawlerParameters.getPath().get());
                    URL reposAsURL = new URL(repos);

                    indexedRevision = getIndexedRevision();
                    logger.info("Indexed Revision Value [{}]", indexedRevision);
                    List<BulkRequestBuilder> bulks = Lists.newArrayList();

                    long lastRevision = SubversionCrawler.getLatestRevision(reposAsURL, crawlerParameters);
                    logger.debug("Checking last revision of repository : {}/{} --> [{}]",
                            reposAsURL, crawlerParameters.getPath().get(), lastRevision);

                    // if indexed revision is the last revision, we have nothing to do
                    // but if it's not, we index the new subversion updates.
                    if (indexedRevision < lastRevision) {
                        UpdatePolicy updatePolicy = getUpdatePolicy(lastRevision, bulkSize);
                        crawlerParameters.setStartRevision(Optional.of(updatePolicy.fromRevision));
                        crawlerParameters.setEndRevision(Optional.of(updatePolicy.toRevision));

                        logger.debug("Indexing repository {}/{} from revision [{}] to [{}] incremental [{}]",
                            reposAsURL, crawlerParameters.getPath().get(),
                            crawlerParameters.getStartRevision().get(),
                            crawlerParameters.getEndRevision().get(),
                            updatePolicy.incremental
                        );

                        // The total list of subversion documents is partitioned
                        // into smaller lists, of max size bulksize
                        List<SubversionRevision> subversionRevisionsBulk =
                                SubversionCrawler.getRevisions(
                                    reposAsURL,
                                    crawlerParameters
                                );
                        // Send the revisions in bulk to the index
                        BulkRequestBuilder bulk = client.prepareBulk();
                        for (SubversionRevision svnRevision : subversionRevisionsBulk) {
                            // First the revision...
                            bulk.add(indexRequest(indexName)
                                    .type(SubversionRevision.TYPE_NAME)
                                    .id(svnRevision.id())
                                    .source(svnRevision.json())
                            );
                            // ... and then the documents/files
                            for (SubversionDocument svnDocument : svnRevision.getDocuments()) {
                                bulk.add(indexRequest(indexName)
                                        .type(SubversionDocument.TYPE_NAME)
                                        .source(svnDocument.json())
                                );
                            }
                            logger.debug("Document added to queue :{}", svnRevision.json());
                        }
                        bulks.add(bulk);
                        totalNumberOfActions += bulk.numberOfActions();
                        executeBulksAndSetLastRevision(totalNumberOfActions,
                            bulks,
                            crawlerParameters.getEndRevision().get()
                        );
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
            logger.debug("Nothing to index (latest revision reached ? [{}]) in {}/{}",
                    indexedRevision, repos, crawlerParameters.getPath().get());
        }
    }

    /**
     * Execute a List of Bulks
     * @param bulks a bunch of bulks
     */
    private void executeBulks(List<BulkRequestBuilder> bulks) {
        for( BulkRequestBuilder bulk : bulks)  {
            // Update the last indexed revision
            try {
                bulk.add(indexRequest(indexName)
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
                logger.info("Updating indexed_revision on index [{}] with id [{}] and value {[{}]:[{}]}",
                        indexName, indexedRevisionID, repos, indexedRevision);
            } catch (IOException e) {
                logger.error("failed to update indexed_revision [{}] on index [{}]" +
                        " because of Exception {}, aborting bulk operation",
                        indexedRevision, indexName, e);
                return;
            }
            logger.info("Indexed revision of repository : {}{} --> [{}]",
                repos, crawlerParameters.getPath().get(), indexedRevision
            );

            try {
                logger.info("Execute bulk {} actions", bulk.numberOfActions());
                BulkResponse response = bulk.execute().actionGet();
                if (response.hasFailures()) {
                    logger.error("failed to execute" + response.buildFailureMessage());
                }
                logger.info("Completed bulk {} actions in {}ms",
                        response.getItems().length,
                        response.getTookInMillis());
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
        // Or if the last indexed revision is inferior
        // to the one in the settings
        // we start from the revision specified
        // in the settings. (default is revision 1)
        UpdatePolicy result = new UpdatePolicy();

        if( indexedRevision == NOT_INDEXED_REVISION
                || indexedRevision < crawlerParameters.getStartRevision().get() ) {
            result.incremental = false;
            if( INDEX_HEAD_REVISION.equals(crawlerParameters.getStartRevision().get()) ) {
                result.fromRevision = lastRevision;
                result.toRevision = lastRevision;
            } else {
                result.fromRevision = crawlerParameters.getStartRevision().get();
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
