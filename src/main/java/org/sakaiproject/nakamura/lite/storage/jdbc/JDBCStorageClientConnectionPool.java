package org.sakaiproject.nakamura.lite.storage.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.ConnectionPoolException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.lite.accesscontrol.CacheHolder;
import org.sakaiproject.nakamura.lite.storage.AbstractClientConnectionPool;
import org.sakaiproject.nakamura.lite.storage.ConcurrentLRUMap;
import org.sakaiproject.nakamura.lite.storage.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

@Component(immediate = true, metatype = true, inherit=true)
@Service(value = ConnectionPool.class)
public class JDBCStorageClientConnectionPool extends AbstractClientConnectionPool {

    private static final Logger LOGGER = LoggerFactory
    .getLogger(JDBCStorageClientConnectionPool.class);

    @Property(value = { "jdbc:derby:sparsemap/db;create=true" })
    public static final String CONNECTION_URL = "jdbc-url";
    @Property(value = { "org.apache.derby.jdbc.EmbeddedDriver" })
    public static final String JDBC_DRIVER = "jdbc-driver";
    
    @Property(value = { "sa" })
    private static final String USERNAME = "username";
    @Property(value = { "" })
    private static final String PASSWORD = "password";

    
    private static final String BASESQLPATH = "org/sakaiproject/nakamura/lite/storage/jdbc/config/client";


    public class JCBCStorageClientConnection implements PoolableObjectFactory {

        private String url;
        private Properties connectionProperties;
        private String username;
        private String password;

        public JCBCStorageClientConnection(Map<String, Object> config) {
            connectionProperties = getConnectionProperties(config);
            username = StorageClientUtils.getSetting(config.get(USERNAME), "");
            password = StorageClientUtils.getSetting(config.get(PASSWORD), "");
            url = getConnectionUrl(config);
        }

        public void activateObject(Object obj) throws Exception {
            JDBCStorageClient client = checkSchema(obj);
            client.activate();
        }

        public void destroyObject(Object obj) throws Exception {
            JDBCStorageClient client = (JDBCStorageClient) obj;
            client.close();

        }

        public Object makeObject() throws Exception {
            if ("".equals(username)) {
                Connection connection = DriverManager.getConnection(url, connectionProperties);
                return checkSchema(new JDBCStorageClient(connection, properties,
                        getSqlConfig(connection)));
            } else {
                Connection connection = DriverManager.getConnection(url, username, password);
                return checkSchema(new JDBCStorageClient(connection, properties,
                        getSqlConfig(connection)));
            }
        }

        public void passivateObject(Object obj) throws Exception {
            JDBCStorageClient client = (JDBCStorageClient) obj;
            client.passivate();
        }

        public boolean validateObject(Object obj) {
            JDBCStorageClient client = checkSchema(obj);
            return client.validate();
        }

    }

    private Map<String, Object> properties;
    private boolean schemaHasBeenChecked = false;
    private Map<String, Object> sqlConfig;
    private Object sqlConfigLock = new Object();

    private Map<String, CacheHolder> sharedCache;

    @Activate
    public void activate(Map<String, Object> properties) throws ClassNotFoundException {
        this.properties = properties;
        super.activate(properties);
        
        sharedCache = new ConcurrentLRUMap<String, CacheHolder>(10000);
        
        String jdbcDriver = (String) properties.get(JDBC_DRIVER);
        Class<?> clazz = Class.forName(jdbcDriver);
        
        LOGGER.info("Loaded Database Driver {} as {}  ",jdbcDriver, clazz);
        
        try {
            @SuppressWarnings("unused")
            JDBCStorageClient client = (JDBCStorageClient) openConnection();
        } catch (ConnectionPoolException e) {
            LOGGER.warn("Failed to check Schema", e);
        } finally {
            try {
                closeConnection();
            } catch (ConnectionPoolException e) {
                LOGGER.warn("Failed to close connection after schema check ", e);
            }
        }

    }
    
    @Deactivate
    public void deactivate(Map<String, Object> properties) {
        super.deactivate(properties);
        
        String connectionUrl = (String) this.properties.get(CONNECTION_URL);
        String jdbcDriver = (String) properties.get(JDBC_DRIVER);
        if ( "org.apache.derby.jdbc.EmbeddedDriver".equals(jdbcDriver) && connectionUrl != null) {
            // need to shutdown this instance.
            String[] parts = StringUtils.split(connectionUrl,';');
            try {
                DriverManager.getConnection(parts[0]+";shutdown=true");
            } catch (SQLException e ) {
                // yes really see http://db.apache.org/derby/manuals/develop/develop15.html#HDRSII-DEVELOP-40464
                LOGGER.info("Sparse Map Content Derby Embedded instance shutdown sucessfully {}",e.getMessage());
            }
        }
    }

    protected JDBCStorageClient checkSchema(Object o) {
        JDBCStorageClient client = (JDBCStorageClient) o;
        if (!schemaHasBeenChecked) {
            synchronized (sqlConfigLock) {
                if (!schemaHasBeenChecked) {
                    try {
                        Connection connection = client.getConnection();
                        DatabaseMetaData metadata = connection.getMetaData();
                        LOGGER.info("Starting Sparse Map Content database ");
                        LOGGER.info("   Database Vendor: {} {}", metadata.getDatabaseProductName(), metadata.getDatabaseProductVersion());
                        LOGGER.info("   Database Driver: {} ", properties.get(JDBC_DRIVER));
                        LOGGER.info("   Database URL   : {} ", properties.get(CONNECTION_URL));
                        client.checkSchema(getClientConfigLocations(client.getConnection()));
                        schemaHasBeenChecked = true;
                    } catch (Throwable e) {
                        LOGGER.warn("Failed to check Schema", e);
                    }
                }
            }
        } else {
            client.setAlive();
        }
        return client;
    }

    public Map<String, Object> getSqlConfig(Connection connection) {
        if (sqlConfig == null) {
            synchronized (sqlConfigLock) {
                if (sqlConfig == null) {
                    try {

                        for (String clientSQLLocation : getClientConfigLocations(connection)) {
                            String clientConfig = clientSQLLocation + ".sql";
                            InputStream in = this.getClass().getClassLoader()
                                    .getResourceAsStream(clientConfig);
                            if (in != null) {
                                try {
                                    Properties p = new Properties();
                                    p.load(in);
                                    in.close();
                                    Builder<String, Object> b = ImmutableMap.builder();
                                    for (Entry<Object, Object> e : p.entrySet()) {
                                        b.put(String.valueOf(e.getKey()), e.getValue());
                                    }
                                    sqlConfig = b.build();
                                    LOGGER.info("Using SQL configuation from {} ", clientConfig);
                                    break;
                                } catch (IOException e) {
                                    LOGGER.info("Failed to read {} ", clientConfig, e);
                                }
                            } else {
                                LOGGER.info("No SQL configuation at {} ", clientConfig);
                            }
                        }
                    } catch (SQLException e) {
                        LOGGER.error("Failed to locate SQL configuration");
                    }
                }
            }
        }
        return sqlConfig;
    }

    private String[] getClientConfigLocations(Connection connection) throws SQLException {
        String dbProductName = connection.getMetaData().getDatabaseProductName()
                .replaceAll(" ", "");
        int dbProductMajorVersion = connection.getMetaData().getDatabaseMajorVersion();
        int dbProductMinorVersion = connection.getMetaData().getDatabaseMinorVersion();

        return new String[] {
                BASESQLPATH + "." + dbProductName + "." + dbProductMajorVersion + "."
                        + dbProductMinorVersion,
                BASESQLPATH + "." + dbProductName + "." + dbProductMajorVersion,
                BASESQLPATH + "." + dbProductName, BASESQLPATH };
    }

    public String getConnectionUrl(Map<String, Object> config) {
        return (String) config.get(CONNECTION_URL);
    }

    public Properties getConnectionProperties(Map<String, Object> config) {
        Properties connectionProperties = new Properties();
        for (Entry<String, Object> e : config.entrySet()) {
            connectionProperties.put(e.getKey(), e.getValue());
        }
        return connectionProperties;
    }

    @Override
    protected PoolableObjectFactory getConnectionPoolFactory() {
        return new JCBCStorageClientConnection(properties);
    }

    @Override
    public Map<String, CacheHolder> getSharedCache() {
        return sharedCache;
    }

}
