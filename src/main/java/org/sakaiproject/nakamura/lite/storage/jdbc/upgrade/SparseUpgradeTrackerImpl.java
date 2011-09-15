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
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.UpgradeTracker;
import org.sakaiproject.nakamura.api.lite.Upgrader;
import org.sakaiproject.nakamura.lite.storage.StorageClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Component(immediate = true, enabled = true, metatype = true)
@Service(value = UpgradeTracker.class)
public class SparseUpgradeTrackerImpl implements UpgradeTracker<StorageClient> {

  private static final UpgraderComparator COMPARATOR = new UpgraderComparator();

  private List<Upgrader<StorageClient>> upgraders = new ArrayList<Upgrader<StorageClient>>();

  public List<Upgrader<StorageClient>> getUpgraders() {
    Collections.sort(this.upgraders, COMPARATOR);
    return this.upgraders;
  }

  public void register(Upgrader<StorageClient> upgrader) {
    this.upgraders.add(upgrader);
  }

  private static class UpgraderComparator implements Comparator<Upgrader> {
    public int compare(Upgrader a, Upgrader b) {
      if (a.getOrder() == null) {
        return 1;
      }
      if (b.getOrder() == null) {
        return -1;
      }
      return a.getOrder().compareTo(b.getOrder());
    }
  }
}
