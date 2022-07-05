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


/**
 * Creates a new analytics view.
 */
@DatabaseChange(name="createAnalyticsView", description = "Create Analytics View", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateAnalyticsViewChange extends DdmAbstractViewChange {

    public DdmCreateAnalyticsViewChange() {
        super();
    }

    public DdmCreateAnalyticsViewChange(String name) {
        super(name);
    }

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
        updateColumnData();

        if (DdmUtils.hasPubContext(this.getChangeSet())){
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

        if (DdmUtils.hasSubContext(this.getChangeSet())) {
            statements.add(new RawSqlStatement("GRANT SELECT ON " + statement.getViewName() + " TO analytics_admin;"));
        }

        return statements.toArray(new SqlStatement[0]);
    }

    @Override
    protected Change[] createInverses() {
        DdmDropAnalyticsViewChange inverse = new DdmDropAnalyticsViewChange();
        inverse.setName(getName());
        return new Change[]{ inverse };
    }

    @Override
    public String getConfirmationMessage() {
        return "Analytics View " + getName() + " created";
    }

}
