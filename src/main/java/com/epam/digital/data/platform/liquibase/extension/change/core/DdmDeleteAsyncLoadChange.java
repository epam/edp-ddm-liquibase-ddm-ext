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
import com.epam.digital.data.platform.liquibase.extension.change.DdmDeleteEntityConfig;
import java.util.ArrayList;
import java.util.List;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;

/**
 * Removes a records in metadata table, which contains names of entities which may be uploaded asynchronously .
 */
@DatabaseChange(name="deleteAsyncLoad", description = "Delete Async Load", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmDeleteAsyncLoadChange extends AbstractChange {

    private String name;
    private List<DdmDeleteEntityConfig> entityList = new ArrayList<>();
    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor)
        throws ParsedNodeException {
        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_NAME)) {
                setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
            } else {

                for(ParsedNode childItem : child.getChildren()) {
                    DdmDeleteEntityConfig entity = new DdmDeleteEntityConfig();
                    entity.load(childItem, resourceAccessor);
                    this.entityList.add(entity);
                }
            }
        }
    }
    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));
        return validationErrors;
    }
    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        for(DdmDeleteEntityConfig entity : entityList) {
            statements.add(DdmUtils.deleteMetadataByAttrNameSql(entity.getName()));
        }
        return statements.toArray(new SqlStatement[0]);
    }

    public void setEntityList(
        List<DdmDeleteEntityConfig> entityList) {
        this.entityList = entityList;
    }

    @Override
    public String getConfirmationMessage() {
        return "Async Load " + getName() + " deleted";
    }
    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }

    @DatabaseChangeProperty(description = "Name of the Async Load to deleted")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<DdmDeleteEntityConfig> getEntityList() {
        return entityList;
    }
}
