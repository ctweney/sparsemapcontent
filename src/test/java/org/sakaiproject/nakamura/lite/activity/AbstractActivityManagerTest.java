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

package org.sakaiproject.nakamura.lite.activity;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.lite.LoggingStorageListener;
import org.sakaiproject.nakamura.lite.accesscontrol.AccessControlManagerImpl;
import org.sakaiproject.nakamura.lite.accesscontrol.AuthenticatorImpl;
import org.sakaiproject.nakamura.lite.content.AbstractContentManagerTest;
import org.sakaiproject.nakamura.lite.content.ContentManagerImpl;

import java.util.Iterator;
import java.util.Map;

public abstract class AbstractActivityManagerTest extends AbstractContentManagerTest {

    @Test
    public void testCreateActivity() throws StorageClientException, AccessDeniedException {
        AuthenticatorImpl AuthenticatorImpl = new AuthenticatorImpl(client, configuration, null);
        User currentUser = AuthenticatorImpl.authenticate("admin", "admin");

        AccessControlManagerImpl accessControlManager = new AccessControlManagerImpl(client,
                currentUser, configuration, null,  new LoggingStorageListener(), principalValidatorResolver);

        ActivityManagerImpl activityManager = new ActivityManagerImpl(client, accessControlManager,
                configuration, null,  new LoggingStorageListener());

        activityManager.update(new Content("/testCreateContent", ImmutableMap.of("prop1", (Object) "value1")));

        Content content = activityManager.get("/testCreateContent");
        Assert.assertEquals("/testCreateContent", content.getPath());

    }

}
