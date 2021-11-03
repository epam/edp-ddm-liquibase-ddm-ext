package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import java.util.stream.Collectors;

import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmPartialUpdateStatement;
import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Table;

public class DdmPartialUpdateGenerator extends AbstractSqlGenerator<DdmPartialUpdateStatement> {

    private final SnapshotGeneratorFactory snapshotGeneratorFactory;

    public DdmPartialUpdateGenerator() {
        this(SnapshotGeneratorFactory.getInstance());
    }

    public DdmPartialUpdateGenerator(SnapshotGeneratorFactory instance) {
        snapshotGeneratorFactory = instance;
    }

    @Override
    public Sql[] generateSql(DdmPartialUpdateStatement statement, Database database, SqlGeneratorChain<DdmPartialUpdateStatement> sqlGeneratorChain) {
        for (DdmTableConfig table : statement.getTables()) {
            Table snapshotTable = null;

            try {
                snapshotTable = snapshotGeneratorFactory.createSnapshot(new Table(null, table.getSchemaName(), table.getName()), database);
            } catch (DatabaseException | InvalidExampleException e) {
                Scope.getCurrentScope().getLog(database.getClass()).info("Cannot create snapshotTable", e);
            }

            if (snapshotTable == null) {
                throw new UnexpectedLiquibaseException("Table " + table.getName() + " does not exist");
            }
        }

        String buffer = "";
        for (DdmTableConfig table : statement.getTables()) {
            buffer = table.getColumns().stream().map(column -> String.valueOf(
                    DdmUtils.insertMetadataSql("partialUpdate", statement.getName(), table.getName(), column.getName())))
                .collect(Collectors.joining());
        }

        return new Sql[]{ new UnparsedSql(buffer) };
    }

    @Override
    public ValidationErrors validate(DdmPartialUpdateStatement statement, Database database, SqlGeneratorChain<DdmPartialUpdateStatement> sqlGeneratorChain) {
        return new ValidationErrors();
    }
}
