/*
 * Copyright 2022 EPAM Systems.
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

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableReadParametersConfig;
import liquibase.change.AbstractChange;
import liquibase.change.AddColumnConfig;
import liquibase.change.ChangeMetaData;
import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddColumnChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Creates a new options for table reads.
 */
@DatabaseChange(name="tableReadParameters", description = "Create Nested Reads For Tables", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmTableReadParametersChange extends AbstractChange {

    private String table;
    private List<DdmTableReadParametersConfig> readParameters = new ArrayList<>();

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        List<DdmCreateTableChange> createTableChanges =
                DdmUtils.getCreateTableChangesFromChangeLog(getChangeSet(), Collections.singletonList(table));
        List<AddColumnChange> columnChanges =
                DdmUtils.getColumnChangesFromChangeLog(this.getChangeSet(), Collections.singletonList(table));
        for (DdmTableReadParametersConfig readParameter : readParameters) {
            ColumnConfig columnConfig = findTableColumn(readParameter, createTableChanges, columnChanges);
            if (columnConfig == null) {
                validationErrors.addError("Column " + readParameter.getName() + "in table " + table + " doesn't exist");
            } else {
                validationErrors.addAll(DdmUtils.validationForNestedReadColumn(getChangeSet(), table, columnConfig));
            }
        }
        return validationErrors;
    }

    private ColumnConfig findTableColumn(DdmTableReadParametersConfig readParametersConfig,
                                         List<DdmCreateTableChange> createTableChanges,
                                         List<AddColumnChange> columnChanges) {

        Optional<ColumnConfig> columnFromCreateTable =
                createTableChanges
                        .stream()
                        .flatMap(tableChange -> tableChange.getColumns().stream())
                        .filter(column -> column.getName().equals(readParametersConfig.getName()))
                        .findFirst();
        if (columnFromCreateTable.isPresent()) {
            return columnFromCreateTable.get();
        }
        Optional<AddColumnConfig> columnFromAddColumn =
                columnChanges
                        .stream()
                        .flatMap(tableChange -> tableChange.getColumns().stream())
                        .filter(column -> column.getName().equals(readParametersConfig.getName()))
                        .findFirst();
        return columnFromAddColumn.orElse(null);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        List<DdmCreateTableChange> createTableChanges =
                DdmUtils.getCreateTableChangesFromChangeLog(getChangeSet(), Collections.singletonList(table));
        List<AddColumnChange> columnChanges =
                DdmUtils.getColumnChangesFromChangeLog(this.getChangeSet(), Collections.singletonList(table));
        for (DdmTableReadParametersConfig readParameter : readParameters) {
            ColumnConfig columnConfig = findTableColumn(readParameter, createTableChanges, columnChanges);
            if (columnConfig == null) {
                continue;
            }
            if (DdmConstants.ATTRIBUTE_FETCH_TYPE_ENTITY.equals(readParameter.getFetchType())) {
                DdmCreateMany2ManyChange m2mChange = DdmUtils.getM2mChangeFromChangelogForNestedRead(
                        getChangeSet(), table, columnConfig.getName());
                if (m2mChange != null) {
                    statements.add(DdmUtils.insertMetadataSql(
                            DdmConstants.SEARCH_METADATA_NESTED_READ, getTable(), m2mChange.getReferenceTableName(), columnConfig.getName()));
                } else if (columnConfig.getConstraints() != null && columnConfig.getConstraints().getForeignKeyName() != null) {
                    statements.add(DdmUtils.insertMetadataSql(
                            DdmConstants.SEARCH_METADATA_NESTED_READ, getTable(), columnConfig.getConstraints().getReferencedTableName(), columnConfig.getName()));
                }
            } else if (DdmConstants.ATTRIBUTE_FETCH_TYPE_ID.equals(readParameter.getFetchType())) {
                statements.add(DdmUtils.deleteMetadataSql(DdmConstants.SEARCH_METADATA_NESTED_READ, getTable(), columnConfig.getName()));
            }
        }
        return statements.toArray(new SqlStatement[0]);
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);
        List<DdmTableReadParametersConfig> readParametersList = new ArrayList<>();

        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_COLUMN)) {
                DdmTableReadParametersConfig readParameter = new DdmTableReadParametersConfig();
                readParameter.load(child, resourceAccessor);
                readParametersList.add(readParameter);
            }
        }
        setReadParameters(readParametersList);
    }

    @Override
    public String getConfirmationMessage() {
        return String.format("Nested Read Metadata for %s created", getTable());
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<DdmTableReadParametersConfig> getReadParameters() {
        return readParameters;
    }

    public void setReadParameters(List<DdmTableReadParametersConfig> readParameters) {
        this.readParameters = readParameters;
    }
}
