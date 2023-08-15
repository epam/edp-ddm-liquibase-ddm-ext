/*
 * Copyright 2023 EPAM Systems.
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
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLogicOperatorConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLogicOperatorSerializableConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLogicOperatorTableConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLogicOperatorTableSerializableConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateAbstractViewStatement;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Creates a new search condition.
 */
@DatabaseChange(name = "createSearchCondition", description = "Create Search Condition", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateSearchConditionChange extends DdmAbstractViewChange {

    private String readMode;

    public DdmCreateSearchConditionChange() {
        super();
    }

    public DdmCreateSearchConditionChange(String name) {
        super(name);
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));
        if (!DdmUtils.isSearchConditionChangeSet(this.getChangeSet())) {
            validationErrors.addError(DdmUtils.printConsistencyChangeSetError(getChangeSet().getId()));
        }
        for (DdmTableConfig table : getTables()) {
            for (DdmColumnConfig column : table.getColumns()) {
                if (DdmConstants.ATTRIBUTE_FETCH_TYPE_ENTITY.equals(column.getFetchType())) {
                    validationErrors.addAll(DdmUtils.validationForNestedReadColumn(getChangeSet(), table.getName(), column));
                }
            }
            DdmLogicOperatorTableConfig tableLogicOperator = table.getTableLogicOperator();
            if (Objects.nonNull(tableLogicOperator)) {
                validationErrors.addAll(validationForLogicOperatorsColumns(tableLogicOperator.getLogicOperators()));
            }
        }
        return validationErrors;
    }

    private ValidationErrors validationForLogicOperatorsColumns(List<DdmLogicOperatorConfig> logicOperators) {
        ValidationErrors errors = new ValidationErrors();
        if (Objects.nonNull(logicOperators)) {
            for (DdmLogicOperatorConfig logicOperator : logicOperators) {
                List<DdmColumnConfig> columns = logicOperator.getColumns();
                if (Objects.nonNull(columns)) {
                    for (DdmColumnConfig column : columns) {
                        if (DdmUtils.isBlank(column.getSearchType())) {
                            errors.addError(String.format("Column [%s] that is used inside logicOperator for SC %s must have a searchType attribute", column.getName(), getName()));
                        }
                    }
                }
                errors.addAll(validationForLogicOperatorsColumns(logicOperator.getLogicOperators()));
            }
        }
        return errors;

    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        updateColumnData();

        if (DdmUtils.hasSubContext(this.getChangeSet())) {
            this.getChangeSet().setIgnore(true);
            return new SqlStatement[0];
        }

        List<SqlStatement> statements = new ArrayList<>();
        DdmCreateAbstractViewStatement statement = generateCreateAbstractViewStatement();
        statement.setCtes(getCtes());
        statement.setTables(getTables());
        statement.setJoins(getJoins());
        statement.setIndexing(getIndexing());
        statement.setLimit(getLimit());
        statement.setConditions(getConditions());

        statements.add(statement);
        statements.add(new RawSqlStatement("GRANT SELECT ON " + statement.getViewName() + " TO application_role;"));

        //  create insert statements for metadata table
        for (DdmTableConfig table : getTables()) {
            table.getFunctions().stream().map(function ->
                    DdmUtils.insertMetadataSql(getName(), table.getName(), function.getName(),
                            function.getAlias())).forEach(statements::add);
            addStatementForColumns(table.getColumns(), statements, table.getName());
            if (Objects.nonNull(table.getTableLogicOperator())) {
                addStatementForNestedColumnsFromLogicOperators(
                        table.getTableLogicOperator().getLogicOperators(), table.getName(), statements);
            }
        }

        if (getLimit() != null && !getLimit().equalsIgnoreCase(DdmConstants.ATTRIBUTE_ALL)) {
            statements.add(insertSearchConditionMetadata(DdmConstants.SEARCH_METADATA_ATTRIBUTE_NAME_LIMIT, getLimit()));
        }

        if (getPagination() != null) {
            statements.add(
                    insertSearchConditionMetadata(DdmConstants.SEARCH_METADATA_ATTRIBUTE_NAME_PAGINATION, getPagination()));
        }

        if (DdmConstants.ATTRIBUTE_ASYNC.equals(getReadMode())) {
            statements.add(DdmUtils.insertMetadataSql(DdmConstants.READ_MODE_CHANGE_TYPE, createChangeMetaData().getName(), getName(), DdmConstants.ATTRIBUTE_ASYNC));
        }

        List<DdmLogicOperatorTableConfig> tableLogicOperators = getTables().stream()
                .map(DdmTableConfig::getTableLogicOperator)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (!tableLogicOperators.isEmpty()) {
            Map<String, List<DdmLogicOperatorTableSerializableConfig>> operations = Collections.singletonMap("operations", convertToSerializableTableLogicOperators(tableLogicOperators));
            String logicOperationsAsString = DdmUtils.convertObjectToString(operations);
            statements.add(DdmUtils.insertMetadataSql(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE, getName(), DdmConstants.ATTRIBUTE_LOGIC_OPERATOR, logicOperationsAsString));
        }

        return statements.toArray(new SqlStatement[0]);
    }

    private List<DdmLogicOperatorTableSerializableConfig> convertToSerializableTableLogicOperators(List<DdmLogicOperatorTableConfig> tableLogicOperators) {
        List<DdmLogicOperatorTableSerializableConfig> serializableTableLogicOperators = new ArrayList<>();
        for (DdmLogicOperatorTableConfig tableLogicOperator : tableLogicOperators) {
            DdmLogicOperatorTableSerializableConfig serializableLogicOperator = new DdmLogicOperatorTableSerializableConfig();
            serializableLogicOperator.setTableName(tableLogicOperator.getTableName());
            serializableLogicOperator.setLogicOperators(convertToSerializableLogicOperators(tableLogicOperator.getLogicOperators()));
            serializableTableLogicOperators.add(serializableLogicOperator);
        }
        return serializableTableLogicOperators;
    }

    private List<DdmLogicOperatorSerializableConfig> convertToSerializableLogicOperators(List<DdmLogicOperatorConfig> logicOperators) {
        List<DdmLogicOperatorSerializableConfig> serializableLogicOperators = new ArrayList<>();
        for (DdmLogicOperatorConfig logicOperator : logicOperators) {
            DdmLogicOperatorSerializableConfig serializableLogicOperator = new DdmLogicOperatorSerializableConfig();
            serializableLogicOperator.setType(logicOperator.getType());
            List<String> columnNames = logicOperator.getColumns().stream()
                    .map(DdmColumnConfig::getAliasOrName)
                    .collect(Collectors.toList());
            serializableLogicOperator.setColumns(columnNames);
            if (Objects.nonNull(logicOperator.getLogicOperators()) && !logicOperator.getLogicOperators().isEmpty()) {
                serializableLogicOperator.setLogicOperators(
                        convertToSerializableLogicOperators(logicOperator.getLogicOperators()));
            }
            serializableLogicOperators.add(serializableLogicOperator);
        }
        return serializableLogicOperators;
    }

    private void addStatementForNestedColumnsFromLogicOperators(List<DdmLogicOperatorConfig> logicOperators, String tableName, List<SqlStatement> statements) {
        for (DdmLogicOperatorConfig logicOperator : logicOperators) {
            addStatementForColumns(logicOperator.getColumns(), statements, tableName);
            if (Objects.nonNull(logicOperator.getLogicOperators()) && !logicOperator.getLogicOperators().isEmpty()) {
                addStatementForNestedColumnsFromLogicOperators(logicOperator.getLogicOperators(), tableName, statements);
            }
        }
    }

    private void addStatementForColumns(List<DdmColumnConfig> columns, List<SqlStatement> statements, String tableName) {
        for (DdmColumnConfig column : columns) {
            statements.add(insertSearchConditionMetadata(DdmConstants.ATTRIBUTE_COLUMN, column.getAliasOrName()));
            if (Boolean.TRUE.equals(column.getReturning())) {
                statements.add(DdmUtils.insertMetadataSql(getName(), tableName, column.getName(), column.getAliasOrName()));
                if (!Objects.isNull(column.getFetchType())) {
                    statements.addAll(createStatementForColumnFetchType(tableName, column));
                }
            }
            if (Objects.nonNull(column.getSearchType())) {
                String metadataAttribute = DdmUtils.mapLiquibaseSearchTypeToMetadataType(column);
                statements.add(insertSearchConditionMetadata(metadataAttribute, column.getAliasOrName()));
            }
        }
    }

    private RawSqlStatement insertSearchConditionMetadata(String attributeName, String attributeValue) {
        return DdmUtils.insertMetadataSql(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE, getName(), attributeName, attributeValue);
    }

    private List<RawSqlStatement> createStatementForColumnFetchType(String tableName, DdmColumnConfig column) {
        List<RawSqlStatement> statements = new ArrayList<>();
        if (DdmConstants.ATTRIBUTE_FETCH_TYPE_ENTITY.equals(column.getFetchType())) {
            DdmCreateMany2ManyChange m2mChange = DdmUtils.getM2mChangeFromChangelogForNestedRead(
                    getChangeSet(), tableName, column.getName());
            if (m2mChange != null) {
                statements.add(DdmUtils.insertMetadataSql(
                        DdmConstants.SEARCH_METADATA_NESTED_READ, getName(), m2mChange.getReferenceTableName(), column.getAliasOrName()));
            } else if (column.getConstraints() != null && column.getConstraints().getForeignKeyName() != null) {
                statements.add(DdmUtils.insertMetadataSql(
                        DdmConstants.SEARCH_METADATA_NESTED_READ, getName(), column.getConstraints().getReferencedTableName(), column.getAliasOrName()));
            }
        }
        return statements;
    }

    @Override
    protected Change[] createInverses() {
        DdmDropSearchConditionChange inverse = new DdmDropSearchConditionChange();
        inverse.setName(getName());
        return new Change[]{ inverse };
    }

    @Override
    public String getConfirmationMessage() {
        return "Search Condition " + getName() + " created";
    }

    public String getReadMode() {
        return readMode;
    }

    public void setReadMode(String readMode) {
        this.readMode = readMode;
    }
}
