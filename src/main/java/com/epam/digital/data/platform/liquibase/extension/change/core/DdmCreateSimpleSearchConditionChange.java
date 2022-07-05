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
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSimpleSearchConditionStatement;
import java.util.Collections;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddColumnChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new simple search condition.
 */
@DatabaseChange(name="createSimpleSearchCondition", description = "Create Simple Search Condition", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateSimpleSearchConditionChange extends AbstractChange {

    private String name;
    private DdmTableConfig table;
    private DdmColumnConfig searchColumn;
    private Boolean indexing;
    private String limit;
    private String readMode;

    public DdmCreateSimpleSearchConditionChange() {
        super();
    }

    public DdmCreateSimpleSearchConditionChange(String name) {
        super();
        this.name = name.toLowerCase();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        if (Boolean.TRUE.equals(indexing) && getSearchColumn() == null) {
            validationErrors.addError("searchColumn is not defined!");
        }

        if (Boolean.TRUE.equals(indexing) && getSearchColumn() != null && getSearchColumn().getSearchType() == null) {
            validationErrors.addError("searchType is not defined!");
        }
        if (!DdmUtils.isSearchConditionChangeSet(this.getChangeSet())){
            validationErrors.addError(DdmUtils.printConsistencyChangeSetError(getChangeSet().getId()));
        }
        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        if (DdmUtils.hasSubContext(this.getChangeSet())){
            this.getChangeSet().setIgnore(true);
            return new SqlStatement[0];
        }

        updateColumnTypes();

        List<SqlStatement> statements = new ArrayList<>();
        DdmCreateSimpleSearchConditionStatement statement = generateCreateSimpleSearchConditionStatement();
        statement.setTable(getTable());
        statement.setSearchColumn(getSearchColumn());
        statement.setIndexing(getIndexing());
        statement.setLimit(getLimit());

        statements.add(statement);

        //  create insert statement for metadata table
        if (statement.getSearchColumn() != null && statement.getSearchColumn().getSearchType() != null) {
            String metadataAttribute = DdmUtils.mapLiquibaseSearchTypeToMetadataType(statement.getSearchColumn());
            statements.add(DdmUtils.insertMetadataSql(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE,
                    getName(), metadataAttribute, getSearchColumn().getName()));
        }

        if (getLimit() != null && !getLimit().equalsIgnoreCase(DdmConstants.ATTRIBUTE_ALL)) {
            statements.add(DdmUtils.insertMetadataSql(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE, getName(), DdmConstants.SEARCH_METADATA_ATTRIBUTE_NAME_LIMIT, getLimit()));
        }

        if (DdmConstants.ATTRIBUTE_ASYNC.equals(getReadMode())){
            statements.add(DdmUtils.insertMetadataSql(DdmConstants.READ_MODE_CHANGE_TYPE, createChangeMetaData().getName(), getName(), DdmConstants.ATTRIBUTE_ASYNC));
        }

        return statements.toArray(new SqlStatement[0]);
    }

    protected DdmCreateSimpleSearchConditionStatement generateCreateSimpleSearchConditionStatement() {
        return new DdmCreateSimpleSearchConditionStatement(getName());
    }

    @Override
    public String getConfirmationMessage() {
        return "Simple Search Condition " + getName() + " created";
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_TABLE)) {
                DdmTableConfig table = new DdmTableConfig();
                table.setName(child.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
                table.setAlias(child.getChildValue(null, DdmConstants.ATTRIBUTE_ALIAS, String.class));
                setTable(table);

                DdmColumnConfig column = new DdmColumnConfig();
                column.setName(child.getChildValue(null, DdmConstants.ATTRIBUTE_SEARCH_COLUMN, String.class));
                column.setType(child.getChildValue(null, DdmConstants.ATTRIBUTE_TYPE, String.class));
                column.setSearchType(child.getChildValue(null, DdmConstants.ATTRIBUTE_SEARCH_TYPE, String.class));
                setSearchColumn(column);
            }
        }
    }

    protected void updateColumnTypes() {
        List<DdmCreateTableChange> tableChanges =
            DdmUtils.getCreateTableChangesFromChangeLog(this.getChangeSet(),
                Collections.singletonList(getTable().getName()));
        List<AddColumnChange> columnChanges =
            DdmUtils.getColumnChangesFromChangeLog(this.getChangeSet(),
                Collections.singletonList(getTable().getName()));

        updateColumnTypeFromCreateTableChanges(tableChanges);
        updateColumnTypeFromAddColumnChanges(columnChanges);
    }

    private void updateColumnTypeFromCreateTableChanges(List<DdmCreateTableChange> tableChanges) {
        tableChanges.stream()
            .filter(tableChange -> tableChange.getTableName().equals(getTable().getName()))
            .flatMap(tableChange -> tableChange.getColumns().stream())
            .filter(changeColumn -> getSearchColumn().getName().equals(changeColumn.getName()))
            .forEach(changeColumn -> getSearchColumn().setType(changeColumn.getType().toLowerCase()));
    }

    private void updateColumnTypeFromAddColumnChanges(List<AddColumnChange> columnChanges) {
        columnChanges.stream()
            .filter(columnChange -> columnChange.getTableName().equals(getTable().getName()))
            .flatMap(columnChange -> columnChange.getColumns().stream())
            .filter(changeColumn -> getSearchColumn().getName().equals(changeColumn.getName()))
            .forEach(changeColumn -> getSearchColumn().setType(changeColumn.getType().toLowerCase()));
    }

    public DdmTableConfig getTable() {
        return this.table;
    }

    public void setTable(DdmTableConfig table) {
        this.table = table;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name.toLowerCase();
    }

    public Boolean getIndexing() {
        return indexing;
    }

    public void setIndexing(Boolean indexing) {
        this.indexing = indexing;
    }

    public DdmColumnConfig getSearchColumn() {
        return searchColumn;
    }

    public void setSearchColumn(DdmColumnConfig searchColumn) {
        this.searchColumn = searchColumn;
    }

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }

    public String getReadMode() {
        return readMode;
    }

    public void setReadMode(String readMode) {
        this.readMode = readMode;
    }
}
