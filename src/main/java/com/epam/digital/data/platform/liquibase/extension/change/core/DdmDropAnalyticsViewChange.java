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

import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropAnalyticsViewStatement;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;

/**
 * Drop analytics view.
 */
@DatabaseChange(name="dropAnalyticsView", description = "Drop Analytics View", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmDropAnalyticsViewChange extends AbstractChange {

    private String name;

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));
        if (!DdmUtils.isAnalyticsChangeSet(this.getChangeSet())){
            validationErrors.addError(DdmUtils.printConsistencyChangeSetError(getChangeSet().getId()));
        }
        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        if (DdmUtils.hasPubContext(this.getChangeSet())){
            this.getChangeSet().setIgnore(true);
            return new SqlStatement[0];
        }
        return new SqlStatement[]{ new DdmDropAnalyticsViewStatement(getName()) };
    }

    @Override
    public String getConfirmationMessage() {
        return "Analytics View " + getName() + " dropped";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }

    @DatabaseChangeProperty(description = "Name of the Analytics View to drop")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
