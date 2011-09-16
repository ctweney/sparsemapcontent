package org.sakaiproject.nakamura.api.lite;

import org.sakaiproject.nakamura.api.lite.PropertyMigrator;

public interface PropertyMigratorTracker {

    PropertyMigrator[] getPropertyMigrators();

    void bind(PropertyMigrator pm);

    void unbind(PropertyMigrator pm);

}
