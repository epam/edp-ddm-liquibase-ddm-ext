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
import java.util.ArrayList;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.DdmParameters;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.ModifyDataTypeChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.ModifyDataTypeStatement;

@DatabaseChange(
    name="modifyDataType",
    description = "Modify data type",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 50,
    appliesTo = "column")
public class DdmModifyDataTypeChange extends ModifyDataTypeChange {

    private Boolean historyFlag;
    private final DdmParameters parameters = new DdmParameters();

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(DdmUtils.validateHistoryFlag(getHistoryFlag()));
        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        statements.add(new ModifyDataTypeStatement(getCatalogName(), getSchemaName(), getTableName(), getColumnName(), getNewDataType()));

        if (Boolean.TRUE.equals(historyFlag)) {
            String historyTableName = getTableName() + parameters.getHistoryTableSuffix();
            statements.add(new ModifyDataTypeStatement(getCatalogName(), getSchemaName(), historyTableName, getColumnName(), getNewDataType()));
        }

        return statements.toArray(new SqlStatement[0]);
    }

    public Boolean getHistoryFlag() {
        return historyFlag;
    }

    public void setHistoryFlag(Boolean historyFlag) {
        this.historyFlag = historyFlag;
    }
}
