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

package org.sakaiproject.nakamura.lite.storage.jdbc.upgrade;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.UpgradeService;
import org.sakaiproject.nakamura.api.lite.UpgradeTracker;
import org.sakaiproject.nakamura.api.lite.Upgrader;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.lite.SessionImpl;
import org.sakaiproject.nakamura.lite.accesscontrol.AccessControlManagerImpl;
import org.sakaiproject.nakamura.lite.content.BlockSetContentHelper;
import org.sakaiproject.nakamura.lite.storage.DisposableIterator;
import org.sakaiproject.nakamura.lite.storage.SparseRow;
import org.sakaiproject.nakamura.lite.storage.StorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"UnusedDeclaration"})
@Component(immediate = true, enabled = true, metatype = true)
@Service(value = UpgradeService.class)
public class SparseUpgradeServiceImpl implements UpgradeService<StorageClient> {

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

  private static final Logger LOGGER = LoggerFactory.getLogger(SparseUpgradeServiceImpl.class);

  @Property(boolValue = true)
  private static final String DRY_RUN_PROPERTY = "dryRun";

  @Property(boolValue = true)
  private static final String VERIFY_PROPERTY = "verify";

  @Reference
  UpgradeTracker<StorageClient> upgradeTracker;

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

  public void doUpgrade() throws Exception {

    SessionImpl session = null;

    try {
      session = (SessionImpl) repository.loginAdministrative();
      String keySpace = configuration.getKeySpace();
      UpgradeLogger upgradeLogger = new UpgradeLogger(session);

      // find upgraders we need to run
      List<Upgrader<StorageClient>> allUpgraders = upgradeTracker.getUpgraders();
      List<Upgrader<StorageClient>> upgradersToRun = filterUpgraders(allUpgraders, upgradeLogger);
      LOGGER.info("Found {} total upgraders, of which {} need to be run on keyspace {}, dry run is {}",
              new Object[]{allUpgraders.size(), upgradersToRun.size(), keySpace, dryRun});
      for ( Upgrader<StorageClient> upgrader : upgradersToRun ) {
        LOGGER.info("Will run " + upgrader.getClass().getName());
      }

      if (upgradersToRun.size() > 0) {
        // process authorizables
        processColumnFamily(session, keySpace, configuration.getAuthorizableColumnFamily(),
                upgradersToRun, AUTHORIZABLE_KEY_EXTRACTOR, upgradeLogger);

        // process regular content
        processColumnFamily(session, keySpace, configuration.getContentColumnFamily(),
                upgradersToRun, CONTENT_KEY_EXTRACTOR, upgradeLogger);

        // process acls
        processColumnFamily(session, keySpace, configuration.getAclColumnFamily(),
                upgradersToRun, ACL_KEY_EXTRACTOR, upgradeLogger);
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
                                   String columnFamily, List<Upgrader<StorageClient>> upgraders,
                                   KeyExtractor keyExtractor, UpgradeLogger upgradeLogger)
          throws StorageClientException, AccessDeniedException {
    long total = session.getClient().allCount(keySpace, columnFamily);
    LOGGER.info("Processing {} objects in column family {}", new Object[]{total, columnFamily});
    if (total > 0) {
      DisposableIterator<SparseRow> allObjects = session.getClient().listAll(keySpace, columnFamily);
      try {
        long currentRow = 0;
        long changedRows = 0;
        while (allObjects.hasNext()) {
          SparseRow row = allObjects.next();
          currentRow++;
          if (currentRow % 1000 == 0) {
            LOGGER.info("Processed {}% remaining {} ", new Object[]{((currentRow * 100) / total), total - currentRow});
          }
          boolean rowChanged = processRow(session, keySpace, columnFamily, upgraders, keyExtractor, row, upgradeLogger);
          if (rowChanged) {
            changedRows++;
          }
        }
        upgradeLogger.write(session);
        LOGGER.info("Finished processing {} total objects in column family {}, {} rows were updated",
                new Object[]{total, columnFamily, changedRows});

      } finally {
        allObjects.close();
      }
    }
  }

  private boolean processRow(SessionImpl session, String keySpace, String columnFamily,
                             List<Upgrader<StorageClient>> upgraders, KeyExtractor keyExtractor, SparseRow row,
                             UpgradeLogger upgradeLogger) throws StorageClientException {
    boolean rowChanged = false;

    Map<String, Object> properties = row.getProperties();
    String rowID = row.getRowId();
    for (Upgrader<StorageClient> upgrader : upgraders) {
      Map<String, Object> originalProperties = new HashMap<String, Object>(properties);
      boolean thisUpgraderChanged = upgrader.upgrade(rowID, properties);
      if (this.verify) {
        boolean verifySucceeded = upgrader.verify(rowID, originalProperties, properties);
        if (!verifySucceeded) {
          throw new IllegalStateException("Upgrader " + upgrader.getClass().getName() + " verification failed " +
                  "at row " + rowID + ". Original properties " + originalProperties + " , new properties " + properties);
        }
      }
      rowChanged = thisUpgraderChanged || rowChanged;
      if (thisUpgraderChanged) {
        upgradeLogger.log(upgrader, rowID, originalProperties, properties);
        LOGGER.info("Upgrader {} changed row {} with verify {}, properties are now {}",
                new Object[]{upgrader.getClass().getName(), rowID, this.verify, properties});
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
      }
    } else {
      LOGGER.info("Skipped processing row {}, could not find key in {}", rowID, properties);
    }

    return rowChanged;
  }

  private List<Upgrader<StorageClient>> filterUpgraders(List<Upgrader<StorageClient>> upgraders, UpgradeLogger logger) {
    // filter out upgraders that have already run
    List<Upgrader<StorageClient>> filteredUpgraders = new ArrayList<Upgrader<StorageClient>>(upgraders.size());
    for (Upgrader<StorageClient> upgrader : upgraders) {
      if (!logger.hasUpgraderRun(upgrader)) {
        filteredUpgraders.add(upgrader);
      }
    }
    return filteredUpgraders;
  }

}
