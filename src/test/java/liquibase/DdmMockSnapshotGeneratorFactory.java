package liquibase;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import liquibase.database.Database;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.structure.DatabaseObject;

public class DdmMockSnapshotGeneratorFactory extends SnapshotGeneratorFactory {

    private final List<DatabaseObject> cache = new ArrayList<>();

    public DdmMockSnapshotGeneratorFactory(DatabaseObject... objects) {
        cache.addAll(asList(objects));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends DatabaseObject> T createSnapshot(T example, Database database) {
        return (T) cache.stream()
            .filter(x -> x.getName().equals(example.getName()))
            .findAny()
            .orElse(null);
    }
}
