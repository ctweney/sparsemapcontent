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

package org.sakaiproject.nakamura.api.lite;

import java.util.Map;

@SuppressWarnings({"UnusedDeclaration"})
public interface Upgrader<T> {

  /**
   * Perform upgrade by modifying the passed map of properties.
   * @param rowID The unique row ID of the object.
   * @param properties A map of properties that implementations will modify if the object is of interest.
   * @return true If the properties map has been modified; false otherwise.
   */
  boolean upgrade(String rowID, Map<String, Object> properties);

  /**
   * Method used by tests to verify proper operation of this upgrader. Make sure that your implementation of
   * verify() agrees with upgrade()!
   * @param rowID The unique row ID of the object.
   * @param beforeProperties A map of properties before the upgrade
   * @param afterProperties A map of properties after the upgrade
   * @return true if the afterProperties is a correct transformation of beforeProperties; false otherwise.
   */
  boolean verify(String rowID, Map<String, Object> beforeProperties, Map<String, Object> afterProperties);

  /**
   * @return Integer used to sort this Upgrader relative to others; the UpgradeService will run upgrades
   * in ascending order. Return null if ordering doesn't matter to this upgrader. Upgraders with null
   * order will run after all those with non-null order.
   */
  Integer getOrder();

}
