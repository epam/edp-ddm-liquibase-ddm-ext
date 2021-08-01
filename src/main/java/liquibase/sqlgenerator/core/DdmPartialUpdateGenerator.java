package liquibase.sqlgenerator.core;

import static liquibase.DdmUtils.insertMetadataSql;

import java.util.Objects;
import liquibase.Scope;
import liquibase.change.DdmColumnConfig;
import liquibase.change.DdmTableConfig;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DdmPartialUpdateStatement;
import liquibase.structure.core.Table;

public class DdmPartialUpdateGenerator extends AbstractSqlGenerator<DdmPartialUpdateStatement> {

    private SnapshotGeneratorFactory snapshotGeneratorFactory;

    public DdmPartialUpdateGenerator() {
        this(SnapshotGeneratorFactory.getInstance());
    }

    DdmPartialUpdateGenerator(SnapshotGeneratorFactory instance) {
        snapshotGeneratorFactory = instance;
    }

    @Override
    public Sql[] generateSql(DdmPartialUpdateStatement statement, Database database, SqlGeneratorChain<DdmPartialUpdateStatement> sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        for (DdmTableConfig table : statement.getTables()) {
            Table snapshotTable = null;

            try {
                snapshotTable = snapshotGeneratorFactory.createSnapshot(new Table(null, table.getSchemaName(), table.getName()), database);
            } catch (DatabaseException | InvalidExampleException e) {
                Scope.getCurrentScope().getLog(database.getClass()).info("Cannot create snapshotTable", e);
            }

            if (Objects.isNull(snapshotTable)) {
                throw new UnexpectedLiquibaseException("Table " + table.getName() + " does not exist");
            }
        }

        for (DdmTableConfig table : statement.getTables()) {
            for (DdmColumnConfig column : table.getColumns()) {
                buffer.append(insertMetadataSql("partialUpdate", statement.getName(), table.getName(), column.getName(), database));
            }
        }

        return new Sql[]{
            new UnparsedSql(buffer.toString())
        };
    }

    @Override
    public ValidationErrors validate(DdmPartialUpdateStatement statement, Database database, SqlGeneratorChain<DdmPartialUpdateStatement> sqlGeneratorChain) {
        return new ValidationErrors();
    }
}
