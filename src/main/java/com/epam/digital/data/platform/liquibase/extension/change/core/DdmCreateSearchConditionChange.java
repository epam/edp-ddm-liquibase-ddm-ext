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

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
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
import java.util.List;
import java.util.Objects;

/**
 * Creates a new search condition.
 */
@DatabaseChange(name="createSearchCondition", description = "Create Search Condition", priority = ChangeMetaData.PRIORITY_DEFAULT)
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
        if (!DdmUtils.isSearchConditionChangeSet(this.getChangeSet())){
            validationErrors.addError(DdmUtils.printConsistencyChangeSetError(getChangeSet().getId()));
        }
        for (DdmTableConfig table : getTables()) {
            for (DdmColumnConfig column : table.getColumns()) {
                if (DdmConstants.ATTRIBUTE_FETCH_TYPE_ENTITY.equals(column.getFetchType())) {
                    validationErrors.addAll(DdmUtils.validationForNestedReadColumn(getChangeSet(), table.getName(), column));
                }
            }
        }
        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        updateColumnData();

        if (DdmUtils.hasSubContext(this.getChangeSet())){
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
                DdmUtils.insertMetadataSql(getName(), table.getName(), function.getName(), function.getAlias())).forEach(statements::add);
            for (DdmColumnConfig column : table.getColumns()) {
                statements.add(insertSearchConditionMetadata(DdmConstants.ATTRIBUTE_COLUMN, column.getAliasOrName()));

                if (Boolean.TRUE.equals(column.getReturning())) {
                    statements.add(DdmUtils.insertMetadataSql(getName(), table.getName(), column.getName(), column.getAliasOrName()));
                    if (!Objects.isNull(column.getFetchType())) {
                        statements.addAll(createStatementForColumnFetchType(table, column));
                    }
                }

                if (column.getSearchType() != null) {
                    String metadataAttribute = DdmUtils.mapLiquibaseSearchTypeToMetadataType(column);
                    statements.add(insertSearchConditionMetadata(metadataAttribute, column.getAliasOrName()));
                }
            }
        }

        if (getLimit() != null && !getLimit().equalsIgnoreCase(DdmConstants.ATTRIBUTE_ALL)) {
            statements.add(insertSearchConditionMetadata(DdmConstants.SEARCH_METADATA_ATTRIBUTE_NAME_LIMIT, getLimit()));
        }

        if (Boolean.TRUE.equals(getPagination())) {
            statements.add(insertSearchConditionMetadata(DdmConstants.SEARCH_METADATA_ATTRIBUTE_NAME_PAGINATION, Boolean.toString(true)));
        }

        if (DdmConstants.ATTRIBUTE_ASYNC.equals(getReadMode())){
            statements.add(DdmUtils.insertMetadataSql(DdmConstants.READ_MODE_CHANGE_TYPE, createChangeMetaData().getName(), getName(), DdmConstants.ATTRIBUTE_ASYNC));
        }

        return statements.toArray(new SqlStatement[0]);
    }

    private RawSqlStatement insertSearchConditionMetadata(String attributeName, String attributeValue) {
        return DdmUtils.insertMetadataSql(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE, getName(), attributeName, attributeValue);
    }

    private List<RawSqlStatement> createStatementForColumnFetchType(DdmTableConfig table, DdmColumnConfig column) {
        List<RawSqlStatement> statements = new ArrayList<>();
        if (DdmConstants.ATTRIBUTE_FETCH_TYPE_ENTITY.equals(column.getFetchType())) {
            DdmCreateMany2ManyChange m2mChange = DdmUtils.getM2mChangeFromChangelogForNestedRead(
                    getChangeSet(), table.getName(), column.getName());
            if (m2mChange != null) {
                statements.add(DdmUtils.insertMetadataSql(
                        DdmConstants.SEARCH_METADATA_NESTED_READ, getName(), m2mChange.getReferenceTableName(), column.getName()));
            } else if (column.getConstraints() != null && column.getConstraints().getForeignKeyName() != null) {
                statements.add(DdmUtils.insertMetadataSql(
                        DdmConstants.SEARCH_METADATA_NESTED_READ, getName(), column.getConstraints().getReferencedTableName(), column.getName()));
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
