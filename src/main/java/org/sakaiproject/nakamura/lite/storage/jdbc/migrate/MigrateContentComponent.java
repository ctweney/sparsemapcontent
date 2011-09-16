package org.sakaiproject.nakamura.lite.storage.jdbc.migrate;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.MigrationService;
import org.sakaiproject.nakamura.lite.ManualOperationService;

import java.util.Map;

@SuppressWarnings({"UnusedParameters"})
@Component(immediate = true, enabled = false, metatype = true)
@Service(value = ManualOperationService.class)
public class MigrateContentComponent implements ManualOperationService {

    @Reference
    private MigrationService migrationService;

    @Activate
    public void activate(Map<String, Object> properties) throws Exception {
        this.migrationService.doMigration();
    }

}
