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

import java.util.ArrayList;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmPartialUpdateStatement;

/**
 * Creates a new partialUpdate.
 */

@DatabaseChange(name="partialUpdate", description = "partialUpdate - partial update", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmPartialUpdateChange extends AbstractChange {

    private List<DdmTableConfig> tables = new ArrayList<>();
    private String name;

    private ValidationErrors validateDoubledTables() {
        ValidationErrors validationErrors = new ValidationErrors();
        List<String> tables = new ArrayList<>();

        for (DdmTableConfig table : getTables()) {
            if (tables.contains(table.getName())) {
                validationErrors.addError("There is doubled table=" + table.getName());
            } else {
                tables.add(table.getName());
            }
        }
        return validationErrors;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(validateDoubledTables());
        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        DdmPartialUpdateStatement statement = new DdmPartialUpdateStatement(getName());
        statement.setTables(getTables());
        return new SqlStatement[]{ statement };
    }

    @Override
    public String getConfirmationMessage() {
        return "Partial update has been set";
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}