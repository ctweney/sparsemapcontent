/*
 * Licensed to the Sakai Foundation (SF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.sakaiproject.nakamura.lite.storage.jdbc.migrate;

import com.google.common.collect.Maps;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.MigrationService;
import org.sakaiproject.nakamura.api.lite.PropertyMigrator;
import org.sakaiproject.nakamura.api.lite.PropertyMigratorTracker;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.lite.SessionImpl;
import org.sakaiproject.nakamura.lite.accesscontrol.AccessControlManagerImpl;
import org.sakaiproject.nakamura.lite.content.BlockSetContentHelper;
import org.sakaiproject.nakamura.lite.storage.DisposableIterator;
import org.sakaiproject.nakamura.lite.storage.SparseRow;
import org.sakaiproject.nakamura.lite.storage.jdbc.Indexer;
import org.sakaiproject.nakamura.lite.storage.jdbc.JDBCStorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"UnusedDeclaration"})
@Component(immediate = true, enabled = true, metatype = true)
@Service(value = MigrationService.class)
public class MigrationServiceImpl implements MigrationService {

    private interface KeyExtractor {
        String getKey(Map<String, Object> properties);
    }

    private static final KeyExtractor AUTHORIZABLE_KEY_EXTRACTOR = new KeyExtractor() {
        public String getKey(Map<String, Object> properties) {
            if (properties.containsKey(Authorizable.ID_FIELD)) {
                return (String) properties.get(Authorizable.ID_FIELD);
            }
            return null;
        }
    };

    private static final KeyExtractor CONTENT_KEY_EXTRACTOR = new KeyExtractor() {
        @SuppressWarnings({"deprecation"})
        public String getKey(Map<String, Object> properties) {
            if (properties.containsKey(BlockSetContentHelper.CONTENT_BLOCK_ID)) {
                // blocks of a bit stream
                return (String) properties
                        .get(BlockSetContentHelper.CONTENT_BLOCK_ID);
            } else if (properties.containsKey(Content.getUuidField())) {
                // a content item and content block item
                return (String) properties.get(Content.getUuidField());
            } else if (properties.containsKey(Content.STRUCTURE_UUID_FIELD)) {
                // a structure item
                return (String) properties.get(Content.PATH_FIELD);
            }
            return null;
        }
    };

    private static final KeyExtractor ACL_KEY_EXTRACTOR = new KeyExtractor() {
        public String getKey(Map<String, Object> properties) {
            if (properties.containsKey(AccessControlManagerImpl._KEY)) {
                return (String) properties.get(AccessControlManagerImpl._KEY);
            }
            return null;
        }
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationServiceImpl.class);

    @Property(boolValue = true)
    private static final String DRY_RUN_PROPERTY = "dryRun";

    @Property(boolValue = true)
    private static final String VERIFY_PROPERTY = "verify";

    @Reference
    PropertyMigratorTracker migratorTracker;

    @Reference
    Repository repository;

    @Reference
    Configuration configuration;

    boolean dryRun = true;

    boolean verify = false;

    public void activate(Map<String, Object> properties) throws Exception {
        this.dryRun = StorageClientUtils.getSetting(properties.get(DRY_RUN_PROPERTY), true);
        this.verify = StorageClientUtils.getSetting(properties.get(VERIFY_PROPERTY), false);
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public void setVerify(boolean verify) {
        this.verify = verify;
    }

    public void doMigration() throws Exception {

        SessionImpl session = null;

        try {
            session = (SessionImpl) repository.loginAdministrative();
            String keySpace = configuration.getKeySpace();
            MigrationLogger migrationLogger = new MigrationLogger(session);
            Indexer indexer = null;
            if (session.getClient() instanceof JDBCStorageClient) {
                indexer = ((JDBCStorageClient) session.getClient()).getIndexer();
            }

            // find migrators we need to run
            PropertyMigrator[] allMigrators = migratorTracker.getPropertyMigrators();
            PropertyMigrator[] migratorsToRun = filterMigrators(allMigrators, migrationLogger);
            LOGGER.info("Found {} total migrators, of which {} need to be run on keyspace {}, dry run is {}, verify is {}",
                    new Object[]{allMigrators.length, migratorsToRun.length, keySpace, this.dryRun, this.verify});
            for (PropertyMigrator migrator : migratorsToRun) {
                LOGGER.info("Will run " + migrator.getClass().getName());
            }

            if (migratorsToRun.length > 0) {
                // process authorizables
                processColumnFamily(session, keySpace, configuration.getAuthorizableColumnFamily(),
                        migratorsToRun, AUTHORIZABLE_KEY_EXTRACTOR, migrationLogger, indexer);

                // process regular content
                processColumnFamily(session, keySpace, configuration.getContentColumnFamily(),
                        migratorsToRun, CONTENT_KEY_EXTRACTOR, migrationLogger, indexer);

                // process acls
                processColumnFamily(session, keySpace, configuration.getAclColumnFamily(),
                        migratorsToRun, ACL_KEY_EXTRACTOR, migrationLogger, indexer);
            }

        } finally {
            if (session != null) {
                try {
                    session.logout();
                } catch (ClientPoolException e) {
                    LOGGER.error("Error logging out of admin session", e);
                }
            }
        }
    }

    private void processColumnFamily(SessionImpl session, String keySpace,
                                     String columnFamily, PropertyMigrator[] migrators,
                                     KeyExtractor keyExtractor, MigrationLogger migrationLogger,
                                     Indexer indexer)
            throws StorageClientException, AccessDeniedException, SQLException {
        long total = session.getClient().allCount(keySpace, columnFamily);
        LOGGER.info("Processing {} objects in column family {}", new Object[]{total, columnFamily});
        if (total > 0) {
            DisposableIterator<SparseRow> allObjects = session.getClient().listAll(keySpace, columnFamily);
            try {
                long currentRow = 0;
                long changedRows = 0;
                Map<String, PreparedStatement> statementCache = Maps.newHashMap();
                while (allObjects.hasNext()) {
                    SparseRow row = allObjects.next();
                    currentRow++;
                    if (currentRow % 1000 == 0) {
                        LOGGER.info("Processed {}% remaining {} ", new Object[]{((currentRow * 100) / total), total - currentRow});
                    }
                    boolean rowChanged = processRow(session, keySpace, columnFamily, migrators,
                            keyExtractor, row, migrationLogger, indexer, statementCache);
                    if (rowChanged) {
                        changedRows++;
                    }
                }
                migrationLogger.write(session);
                LOGGER.info("Finished processing {} total objects in column family {}, {} rows were updated",
                        new Object[]{total, columnFamily, changedRows});

            } finally {
                allObjects.close();
            }
        }
    }

    private boolean processRow(SessionImpl session, String keySpace, String columnFamily,
                               PropertyMigrator[] migrators, KeyExtractor keyExtractor, SparseRow row,
                               MigrationLogger migrationLogger,
                               Indexer indexer, Map<String, PreparedStatement> statementCache)
            throws StorageClientException, SQLException {
        boolean rowChanged = false;

        Map<String, Object> properties = row.getProperties();
        String rowID = row.getRowId();
        for (PropertyMigrator migrator : migrators) {
            Map<String, Object> originalProperties = new HashMap<String, Object>(properties);
            boolean thisMigratorChanged = migrator.migrate(rowID, properties);
            if (this.verify) {
                boolean verifySucceeded = migrator.verify(rowID, originalProperties, properties);
                if (!verifySucceeded) {
                    throw new IllegalStateException("Migrator " + migrator.getClass().getName() + " verification failed " +
                            "at row " + rowID + ". Original properties " + originalProperties + " , new properties " + properties);
                }
            }
            rowChanged = thisMigratorChanged || rowChanged;
            if (thisMigratorChanged) {
                migrationLogger.log(migrator, rowID, originalProperties, properties);
                LOGGER.info("Migrator {} changed row {} with verify {}, properties are now {}",
                        new Object[]{migrator.getClass().getName(), rowID, this.verify, properties});
            }
        }
        String key = keyExtractor.getKey(properties);
        if (key != null) {
            if (rowChanged) {
                if (!dryRun) {
                    // actually persist changes if this isn't a dry run
                    session.getClient().insert(keySpace, columnFamily, key, properties,
                            false);
                }
            } else {
                if (!dryRun && indexer != null) {
                    // reindex if we have an indexer (only for jdbc storage clients)
                    indexer.index(statementCache, keySpace, columnFamily, key, rowID, properties);
                }
            }
        } else {
            LOGGER.info("Skipped processing row {}, could not find key in {}", rowID, properties);
        }

        return rowChanged;
    }

    private PropertyMigrator[] filterMigrators(PropertyMigrator[] migrators, MigrationLogger logger) {
        // filter out migrators that have already run
        List<PropertyMigrator> filtered = new ArrayList<PropertyMigrator>(migrators.length);
        for (PropertyMigrator migrator : migrators) {
            if (!logger.hasMigratorRun(migrator)) {
                filtered.add(migrator);
            }
        }
        return filtered.toArray(new PropertyMigrator[filtered.size()]);
    }

}
