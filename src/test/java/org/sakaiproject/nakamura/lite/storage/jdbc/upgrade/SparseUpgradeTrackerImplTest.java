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

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.sakaiproject.nakamura.api.lite.UpgradeTracker;
import org.sakaiproject.nakamura.api.lite.Upgrader;
import org.sakaiproject.nakamura.lite.storage.StorageClient;
import org.sakaiproject.nakamura.lite.storage.jdbc.upgrade.SparseUpgradeTrackerImpl;

import java.util.List;

public class SparseUpgradeTrackerImplTest extends Assert {

  private UpgradeTracker<StorageClient> tracker;

  @Mock
  Upgrader<StorageClient> firstMockUpgrader;

  @Mock
  Upgrader<StorageClient> secondMockUpgrader;

  @Mock
  Upgrader<StorageClient> nullMockUpgrader;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    this.tracker = new SparseUpgradeTrackerImpl();
    Mockito.when(firstMockUpgrader.getOrder()).thenReturn(0);
    Mockito.when(secondMockUpgrader.getOrder()).thenReturn(10);
    Mockito.when(nullMockUpgrader.getOrder()).thenReturn(null);

  }

  @Test
  public void getUpgraders() {
    assertTrue(this.tracker.getUpgraders().isEmpty());
  }

  @Test
  public void register() {
    this.tracker.register(firstMockUpgrader);
    assertFalse(this.tracker.getUpgraders().isEmpty());
  }

  @Test
  public void registerSortedUpgraders() {

    // add to tracker in unsorted order
    this.tracker.register(nullMockUpgrader);
    this.tracker.register(secondMockUpgrader);
    this.tracker.register(firstMockUpgrader);

    // see if tracker keeps them in sorted order
    List<Upgrader<StorageClient>> storedUpgraders = this.tracker.getUpgraders();
    assertEquals(firstMockUpgrader, storedUpgraders.get(0));
    assertEquals(secondMockUpgrader, storedUpgraders.get(1));
    assertEquals(nullMockUpgrader, storedUpgraders.get(2));

  }
}
