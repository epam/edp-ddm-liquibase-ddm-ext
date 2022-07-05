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
import java.util.Collections;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateMany2ManyStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new create many to many relation.
 */
@DatabaseChange(name="createMany2Many", description = "Create Many To Many Relation", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateMany2ManyChange extends AbstractChange {

    private String mainTableName;
    private String mainTableKeyField;
    private String referenceTableName;
    private String referenceKeysArray;
    private List<DdmColumnConfig> mainTableColumns = new ArrayList<>();
    private List<DdmColumnConfig> referenceTableColumns = new ArrayList<>();

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));
        if (findPKColumnName(referenceTableName) == null) {
            validationErrors.addError("Table " + referenceTableName +
                " or corresponding primary key column doesn't exist");
        }
        return validationErrors;
    }

    private String findPKColumnName(String referenceTableName) {
        List<DdmCreateTableChange> tableChanges =
            DdmUtils.getCreateTableChangesFromChangeLog(this.getChangeSet(),
                Collections.singletonList(referenceTableName));

        return tableChanges.stream()
            .flatMap(tableChange -> tableChange.getColumns().stream())
            .filter(column -> column.getConstraints() != null &&
                column.getConstraints().getPrimaryKeyName() != null).findFirst()
            .map(ColumnConfig::getName).orElse(null);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        DdmCreateMany2ManyStatement statement = new DdmCreateMany2ManyStatement();
        statement.setMainTableName(getMainTableName());
        statement.setMainTableKeyField(getMainTableKeyField());
        statement.setReferenceTableName(getReferenceTableName());
        statement.setReferenceColumnName(findPKColumnName(referenceTableName));
        statement.setReferenceKeysArray(getReferenceKeysArray());
        statement.setMainTableColumns(getMainTableColumns());
        statement.setReferenceTableColumns(getReferenceTableColumns());
        statements.add(statement);

        return statements.toArray(new SqlStatement[0]);
    }

    public String getRelationName() {
        return mainTableName + "_" + referenceTableName + DdmConstants.SUFFIX_RELATION;
    }

    @Override
    public String getConfirmationMessage() {
        return "Many To Many Relation " + getRelationName() + " created";
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        for (ParsedNode child : parsedNode.getChildren()) {
            DdmTableConfig tableConfig = new DdmTableConfig();
            tableConfig.load(child, resourceAccessor);

            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_MAIN_TABLE_COLUMNS)) {
                setMainTableColumns(tableConfig.getColumns());
            } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_REFERENCE_TABLE_COLUMNS)) {
                setReferenceTableColumns(tableConfig.getColumns());
            }
        }
    }

    public String getMainTableName() {
        return mainTableName;
    }

    public void setMainTableName(String mainTableName) {
        this.mainTableName = mainTableName.toLowerCase();
    }

    public String getMainTableKeyField() {
        return mainTableKeyField;
    }

    public void setMainTableKeyField(String mainTableKeyField) {
        this.mainTableKeyField = mainTableKeyField;
    }

    public String getReferenceTableName() {
        return referenceTableName;
    }

    public void setReferenceTableName(String referenceTableName) {
        this.referenceTableName = referenceTableName.toLowerCase();
    }

    public String getReferenceKeysArray() {
        return referenceKeysArray;
    }

    public void setReferenceKeysArray(String referenceKeysArray) {
        this.referenceKeysArray = referenceKeysArray;
    }

    public List<DdmColumnConfig> getMainTableColumns() {
        return mainTableColumns;
    }

    public void setMainTableColumns(List<DdmColumnConfig> mainTableColumns) {
        this.mainTableColumns = mainTableColumns;
    }

    public List<DdmColumnConfig> getReferenceTableColumns() {
        return referenceTableColumns;
    }

    public void setReferenceTableColumns(List<DdmColumnConfig> referenceTableColumns) {
        this.referenceTableColumns = referenceTableColumns;
    }
}
