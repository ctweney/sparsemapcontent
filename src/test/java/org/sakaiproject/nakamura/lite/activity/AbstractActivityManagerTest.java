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
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.PrincipalValidatorResolver;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.lite.ConfigurationImpl;
import org.sakaiproject.nakamura.lite.LoggingStorageListener;
import org.sakaiproject.nakamura.lite.accesscontrol.AccessControlManagerImpl;
import org.sakaiproject.nakamura.lite.accesscontrol.AuthenticatorImpl;
import org.sakaiproject.nakamura.lite.accesscontrol.PrincipalValidatorResolverImpl;
import org.sakaiproject.nakamura.lite.authorizable.AuthorizableActivator;
import org.sakaiproject.nakamura.lite.content.AbstractContentManagerTest;
import org.sakaiproject.nakamura.lite.content.ContentManagerImpl;
import org.sakaiproject.nakamura.lite.storage.StorageClient;
import org.sakaiproject.nakamura.lite.storage.StorageClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractActivityManagerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractContentManagerTest.class);

    private ActivityManagerImpl activityManager;

    private ContentManagerImpl contentManager;

    @Before
    public void before() throws StorageClientException, AccessDeniedException, ClassNotFoundException, IOException {

        Map<String, Object> properties = Maps.newHashMap();
        properties.put("keyspace", "n");
        properties.put("acl-column-family", "ac");
        properties.put("authorizable-column-family", "au");
        properties.put("content-column-family", "cn");
        ConfigurationImpl configuration = new ConfigurationImpl();
        configuration.activate(properties);
        StorageClientPool clientPool = getClientPool(configuration);
        StorageClient client = clientPool.getClient();
        AuthorizableActivator authorizableActivator = new AuthorizableActivator(client,
                configuration);
        authorizableActivator.setup();
        PrincipalValidatorResolver principalValidatorResolver = new PrincipalValidatorResolverImpl();

        AuthenticatorImpl AuthenticatorImpl = new AuthenticatorImpl(client, configuration, null);
        User currentUser = AuthenticatorImpl.authenticate("admin", "admin");

        AccessControlManagerImpl accessControlManager = new AccessControlManagerImpl(client,
                currentUser, configuration, null, new LoggingStorageListener(), principalValidatorResolver);

        activityManager = new ActivityManagerImpl(client, accessControlManager,
                configuration, null, new LoggingStorageListener());

        contentManager = new ContentManagerImpl(client, accessControlManager,
                configuration, null, new LoggingStorageListener());

        LOGGER.info("Setup Complete");
    }

    protected abstract StorageClientPool getClientPool(Configuration configuration) throws ClassNotFoundException;

    @Test
    public void testActivitySeparateFromContent() throws StorageClientException, AccessDeniedException {

        activityManager.update(new Content("/testActivity", ImmutableMap.<String, Object>of("prop1", "value1")));
        Content activity = activityManager.get("/testActivity");
        Assert.assertEquals("/testActivity", activity.getPath());

        // to make sure activity is stored in separate space from content
        Content content = contentManager.get("/testActivity");
        Assert.assertNull(content);

        contentManager.update(new Content("/testContentManagerContent", ImmutableMap.of("prop1", (Object) "value1")));
        Content cmContent = contentManager.get("/testContentManagerContent");
        Assert.assertNotNull(cmContent);

        Content actContent = activityManager.get("/testContentManagerContent");
        Assert.assertNull(actContent);
    }

    @Test
    public void testChildren() throws Exception {
        activityManager.update(new Content("/some/multi/level/path", ImmutableMap.<String, Object>of("prop", "val")));
        Content path = activityManager.get("/some/multi/level/path");
        Assert.assertNotNull(path);
        Content level = activityManager.get("/some/multi/level");
        Assert.assertNotNull(level);
        Content levelChild = level.listChildren().iterator().next();
        Assert.assertNotNull(levelChild);
        Assert.assertEquals("/some/multi/level/path", levelChild.getPath());
    }
}
