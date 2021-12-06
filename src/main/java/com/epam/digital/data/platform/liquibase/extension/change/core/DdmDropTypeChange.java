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
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropTypeStatement;

/**
 * Drops an existing type.
 */
@DatabaseChange(name="dropType", description = "Drops an existing type", priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "type")
public class DdmDropTypeChange extends AbstractChange {

    private String name;

    @DatabaseChangeProperty(mustEqualExisting = "table", description = "Name of the table to drop")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{ 
            new DdmDropTypeStatement(name),
            DdmUtils.deleteMetadataByChangeTypeAndChangeNameSql(
                DdmConstants.TYPE_METADATA_CHANGE_TYPE_VALUE, name), 
            DdmUtils.deleteMetadataByChangeTypeAndChangeNameSql(
                DdmConstants.TYPE_METADATA_ATTRIBUTE_NAME_LABEL, name)};
    }

    @Override
    public String getConfirmationMessage() {
        return "Type " + name + " dropped";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }
}
