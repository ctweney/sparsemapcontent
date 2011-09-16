package org.sakaiproject.nakamura.lite.storage.jdbc.migrate;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferenceStrategy;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.PropertyMigrator;
import org.sakaiproject.nakamura.api.lite.PropertyMigratorTracker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

@Component(immediate = true, metatype = true)
@Service(value = PropertyMigratorTracker.class)
@Reference(cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE, name = "propertyMigrator", referenceInterface = PropertyMigrator.class, policy = ReferencePolicy.DYNAMIC, strategy = ReferenceStrategy.EVENT, bind = "bind", unbind = "unbind")
public class PropertyMigratorTrackerService implements PropertyMigratorTracker {

    private static final PropertyMigratorComparator COMPARATOR = new PropertyMigratorComparator();

    private final List<PropertyMigrator> propertyMigrators = new Vector<PropertyMigrator>();

    public PropertyMigrator[] getPropertyMigrators() {
        Collections.sort(this.propertyMigrators, COMPARATOR);
        return propertyMigrators.toArray(new PropertyMigrator[propertyMigrators.size()]);
    }

    public void bind(PropertyMigrator pm) {
        propertyMigrators.add(pm);
    }

    public void unbind(PropertyMigrator pm) {
        propertyMigrators.remove(pm);
    }

    private static class PropertyMigratorComparator implements Comparator<PropertyMigrator> {
        public int compare(PropertyMigrator a, PropertyMigrator b) {
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
