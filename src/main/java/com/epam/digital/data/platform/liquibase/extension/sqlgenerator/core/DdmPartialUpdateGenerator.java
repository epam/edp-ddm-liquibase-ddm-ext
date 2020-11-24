/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
