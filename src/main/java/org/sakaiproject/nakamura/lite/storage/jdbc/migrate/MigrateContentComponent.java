package org.sakaiproject.nakamura.lite.storage.jdbc.migrate;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.PropertyMigrator;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.lite.ManualOperationService;
import org.sakaiproject.nakamura.lite.SessionImpl;
import org.sakaiproject.nakamura.lite.accesscontrol.AccessControlManagerImpl;
import org.sakaiproject.nakamura.lite.content.BlockSetContentHelper;
import org.sakaiproject.nakamura.lite.storage.DisposableIterator;
import org.sakaiproject.nakamura.lite.storage.SparseRow;
import org.sakaiproject.nakamura.lite.storage.StorageClient;
import org.sakaiproject.nakamura.lite.storage.jdbc.Indexer;
import org.sakaiproject.nakamura.lite.storage.jdbc.JDBCStorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * This component performs migration for JDBC only. It goes direct to the JDBC
 * tables to get a lazy iterator of rowIDs direct from the StorageClient which
 * it then updates one by one. In general this approach to migration is only
 * suitable for the JDBC drivers since they are capable of producing a non in
 * memory list of rowids, a migrator that targets the ColumDBs should probably
 * use a MapReduce job to perform migration and avoid streaming all data through
 * a single node over the network.
 * 
 * At present, the migrator does not record if an item has been migrated. Which
 * means if a migration operation is stopped it will have to be restarted from
 * the beginning and records that have already been migrated will get
 * re-processed. To put a restart facility in place care will need to taken to
 * ensure that updates to existing rows and new rows are tracked as well as the
 * rows that have already been processed. In addition a performant way of
 * querying all objects to get a dense list of items to be migrated. Its not
 * impossible but needs some careful thought to make it work on realistic
 * datasets (think 100M records+, don't think 10K records)
 * 
 * @author ieb
 * 
 */
@Component(immediate = true, enabled = false, metatype = true)
@Service(value = ManualOperationService.class)
public class MigrateContentComponent implements ManualOperationService {

    private static final String DEFAULT_REDOLOG_LOCATION = "migrationlogs";

    @Property(value=DEFAULT_REDOLOG_LOCATION)
    private static final String PROP_REDOLOG_LOCATION = "redolog-location";

    private static final int DEFAULT_MAX_LOG_SIZE = 1024000;
    
    @Property(intValue=DEFAULT_MAX_LOG_SIZE)
    private static final String PROP_MAX_LOG_SIZE = "max-redo-log-size";


    public interface IdExtractor {

        String getKey(Map<String, Object> properties);

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateContentComponent.class);

    @Reference
    private Repository repository;

    @Reference
    private Configuration configuration;

    @Reference
    private PropertyMigratorTracker propertyMigratorTracker;

    @Property(boolValue = true)
    private static final String PERFORM_MIGRATION = "dryRun";



    @Activate
    public void activate(Map<String, Object> properties) throws StorageClientException,
            AccessDeniedException, IOException {
        boolean dryRun = StorageClientUtils.getSetting(properties.get(PERFORM_MIGRATION), true);
        String redoLogLocation = StorageClientUtils.getSetting(properties.get(PROP_REDOLOG_LOCATION), DEFAULT_REDOLOG_LOCATION);
        int maxLogFileSize = StorageClientUtils.getSetting(properties.get(PROP_MAX_LOG_SIZE), DEFAULT_MAX_LOG_SIZE);
        SessionImpl session = (SessionImpl) repository.loginAdministrative();
        StorageClient client = session.getClient();
        FileRedoLogger migrateRedoLog = new FileRedoLogger(redoLogLocation, maxLogFileSize);
        client.setStorageClientListener(migrateRedoLog);
        try{
            if (client instanceof JDBCStorageClient) {
                JDBCStorageClient jdbcClient = (JDBCStorageClient) client;
                String keySpace = configuration.getKeySpace();
    
                Indexer indexer = jdbcClient.getIndexer();
    
                PropertyMigrator[] propertyMigrators = propertyMigratorTracker.getPropertyMigrators();
                for (PropertyMigrator p : propertyMigrators) {
                    LOGGER.info("DryRun:{} Using Property Migrator {} ", dryRun, p);
                }
                reindex(dryRun, jdbcClient, keySpace, configuration.getAuthorizableColumnFamily(),
                        indexer, propertyMigrators, new IdExtractor() {
    
                            public String getKey(Map<String, Object> properties) {
                                if (properties.containsKey(Authorizable.ID_FIELD)) {
                                    return (String) properties.get(Authorizable.ID_FIELD);
                                }
                                return null;
                            }
                        });
                reindex(dryRun, jdbcClient, keySpace, configuration.getContentColumnFamily(), indexer,
                        propertyMigrators, new IdExtractor() {
    
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
                        });
    
                reindex(dryRun, jdbcClient, keySpace, configuration.getAclColumnFamily(), indexer,
                        propertyMigrators, new IdExtractor() {
                            public String getKey(Map<String, Object> properties) {
                                if (properties.containsKey(AccessControlManagerImpl._KEY)) {
                                    return (String) properties.get(AccessControlManagerImpl._KEY);
                                }
                                return null;
                            }
                        });
            } else {
                LOGGER.warn("This class will only re-index content for the JDBCStorageClients");
            }
        } finally {
            migrateRedoLog.close();
        }
        
    }

    private void reindex(boolean dryRun, StorageClient jdbcClient, String keySpace,
            String columnFamily, Indexer indexer, PropertyMigrator[] propertyMigrators,
            IdExtractor idExtractor) throws StorageClientException {
        long objectCount = jdbcClient.allCount(keySpace, columnFamily);
        LOGGER.info("DryRun:{} Migrating {} objects in {} ", new Object[] { dryRun, objectCount,
                columnFamily });
        if (objectCount > 0) {
            DisposableIterator<SparseRow> allObjects = jdbcClient.listAll(keySpace, columnFamily);
            try {
                long c = 0;
                while (allObjects.hasNext()) {
                    Map<String, PreparedStatement> statementCache = Maps.newHashMap();
                    SparseRow r = allObjects.next();
                    c++;
                    if (c % 1000 == 0) {
                        LOGGER.info("DryRun:{} {}% remaining {} ", new Object[] { dryRun,
                                ((c * 100) / objectCount), objectCount - c });
                    }
                    try {
                        Map<String, Object> properties = r.getProperties();
                        String rid = r.getRowId();
                        boolean save = false;
                        for (PropertyMigrator propertyMigrator : propertyMigrators) {
                            save = propertyMigrator.migrate(rid, properties) || save;
                        }
                        String key = idExtractor.getKey(properties);
                        if (key != null) {
                            if (!dryRun) {
                                if (save) {
                                    jdbcClient.insert(keySpace, columnFamily, key, properties,
                                            false);
                                } else {
                                    indexer.index(statementCache, keySpace, columnFamily, key, rid,
                                            properties);
                                }
                            } else {
                                if (c > 2000) {
                                    LOGGER.info("Dry Run Migration Stoped at 2000 Objects ");
                                    break;
                                }
                            }
                        } else {
                            LOGGER.info("DryRun:{} Skipped Reindexing, no key in  {}", dryRun,
                                    properties);
                        }
                    } catch (SQLException e) {
                        LOGGER.warn(e.getMessage(), e);
                    } catch (StorageClientException e) {
                        LOGGER.warn(e.getMessage(), e);
                    }
                }
            } finally {
                allObjects.close();
            }
        }
    }
}
