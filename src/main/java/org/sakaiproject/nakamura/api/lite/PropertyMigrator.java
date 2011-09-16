package org.sakaiproject.nakamura.api.lite;

import java.util.Map;

/**
 * Implementations of PropertyMigrators registered with OSGi are called by the
 * MigrateContentComponent, when its activated (normally disabled). All
 * registered implementation will be called, once for each Map within the
 * system. If they determine that the map is of the appropriate type and needs
 * modification, they should modify it, and return true. If not they should
 * leave the map untouched. There is no guarantee in what order each migrator
 * might be called. If any PropertyMigrator modifies a set of properties, the
 * map will be resaved under the same key. If no properties are modified by any
 * PropertyMigrators, then the object will be re-indexed with the current index
 * operation. Un filtered access is given to all properties, so anyone
 * implementing this interface must take great care not to break referential
 * integrity of each object or invalidate the internals of the object.
 * <p/>
 * The MigrateContentComponent is not active by default, and should only be made
 * active by an Administrator using the Web UI.
 * <p/>
 * The migrate methods will be called once for every object within the system.
 * (could be billions of times).
 *
 * @author ieb
 */
public interface PropertyMigrator {

    /**
     * @param rowID        the row id of the current object as loaded from the store. If
     *                   the property representing the key for the type of object is
     *                   changed, this object will be saved under a new rowid. The
     *                   calculation of the rowid depends on the storage implementation
     *                   and the value of the key.
     * @param properties a map of properties. Implementations are expected to modify
     *                   this map, and return true if modifications are made.
     * @return true if any modifications were made to properties, false
     *         otherwise.
     */
    boolean migrate(String rowID, Map<String, Object> properties);

    /**
     * Method used by tests to verify proper operation of this upgrader. Make sure that your implementation of
     * verify() agrees with upgrade()!
     *
     * @param rowID            The unique row ID of the object.
     * @param beforeProperties A map of properties before the upgrade
     * @param afterProperties  A map of properties after the upgrade
     * @return true if the afterProperties is a correct transformation of beforeProperties; false otherwise.
     */
    boolean verify(String rowID, Map<String, Object> beforeProperties, Map<String, Object> afterProperties);

    /**
     * @return Integer used to sort this Upgrader relative to others; the UpgradeService will run upgrades
     *         in ascending order. Return null if ordering doesn't matter to this upgrader. Upgraders with null
     *         order will run after all those with non-null order.
     */
    Integer getOrder();

}
