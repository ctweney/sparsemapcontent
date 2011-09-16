package org.sakaiproject.nakamura.api.lite;

public interface PropertyMigratorTracker {

    PropertyMigrator[] getPropertyMigrators();

    void bind(PropertyMigrator pm);

    void unbind(PropertyMigrator pm);

}
