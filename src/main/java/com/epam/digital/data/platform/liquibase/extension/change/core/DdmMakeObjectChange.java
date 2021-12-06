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

package com.epam.digital.data.platform.liquibase.extension.change.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmParameters;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.change.AddColumnConfig;
import liquibase.change.ChangeMetaData;
import liquibase.change.ConstraintsConfig;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.util.JdbcUtils;

/**
 * Make Object.
 */
@DatabaseChange(name="makeObject", description = "Make Object", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmMakeObjectChange extends AbstractChange {

    private List<DdmTableConfig> tables = new ArrayList<>();
    private final DdmParameters parameters = new DdmParameters();
    private final SnapshotGeneratorFactory snapshotGeneratorFactory;

    public DdmMakeObjectChange() {
        this(SnapshotGeneratorFactory.getInstance());
    }

    DdmMakeObjectChange(SnapshotGeneratorFactory instance) {
        snapshotGeneratorFactory = instance;
    }

    private boolean columnExists(Database database, String table, String column) {
        boolean exists = false;

        Statement statement = null;
        ResultSet resultSet = null;

        if (database.getConnection() instanceof JdbcConnection) {
            try {
                statement = ((JdbcConnection) database.getConnection()).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

                String sql = "SELECT EXISTS(SELECT 1 "
                    + "FROM information_schema.columns "
                    + "WHERE table_name='" + table + "' AND column_name='" + column + "') as " + DdmConstants.ATTRIBUTE_COLUMN + ";";

                resultSet = statement.executeQuery(sql);

                if (resultSet.next()) {
                    exists = resultSet.getBoolean(DdmConstants.ATTRIBUTE_COLUMN);
                }
            } catch (SQLException | DatabaseException e) {
                Scope.getCurrentScope().getLog(database.getClass()).info("Cannot select version", e);
            } finally {
                JdbcUtils.close(resultSet, statement);
            }
        }

        return exists;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        ConstraintsConfig fkConstraint = new ConstraintsConfig();
        fkConstraint.setReferencedTableName(parameters.getSubjectTable());
        fkConstraint.setReferencedColumnNames(parameters.getSubjectColumn());

        AddColumnConfig column = new AddColumnConfig();
        column.setName(parameters.getSubjectColumn());
        column.setType(parameters.getSubjectColumnType());
        column.setConstraints(fkConstraint);

        for (DdmTableConfig table : getTables()) {
            if (!columnExists(database, table.getName(), parameters.getSubjectColumn())) {
                DdmAddColumnChange change = new DdmAddColumnChange(snapshotGeneratorFactory);
                change.setChangeSet(getChangeSet());
                change.setHistoryFlag(true);
                change.setTableName(table.getName());

                fkConstraint.setForeignKeyName("fk_" + table.getName() + "_" + parameters.getSubjectTable());

                change.addColumn(column);

                statements.addAll(Arrays.asList(change.generateStatements(database)));
            }
        }

        return statements.toArray(new SqlStatement[0]);
    }

    @Override
    public String getConfirmationMessage() {
        return "Objects have been made";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }

    @Override
    public void load(
        ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_TABLE)) {
                DdmTableConfig table = new DdmTableConfig();
                table.load(child, resourceAccessor);
                addTable(table);
            }
        }
    }

    public void addTable(DdmTableConfig table) {
        this.tables.add(table);
    }

    public List<DdmTableConfig> getTables() {
        return tables;
    }

    public void setTables(List<DdmTableConfig> tables) {
        this.tables = tables;
    }
}