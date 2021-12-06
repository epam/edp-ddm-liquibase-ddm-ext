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

import com.epam.digital.data.platform.liquibase.extension.DdmParameters;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmUndistributeTableStatement;
import liquibase.util.StringUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new Undistribute Table.
 */
@DatabaseChange(name="undistributeTable", description = "Undistribute Table", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmUndistributeTableChange extends AbstractChange {
    private String tableName;
    private String scope;

    public DdmUndistributeTableChange() {
        super();
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

        if (StringUtil.isEmpty(getScope())
                || DdmParameters.isAll(getScope())
                || DdmParameters.isPrimary(getScope())) {
            statements.add(generateUndistributeTableStatement(getTableName()));
        }

        if (getScope() != null && (DdmParameters.isAll(getScope()) || DdmParameters.isHistory(getScope()))) {
            DdmParameters parameters = new DdmParameters();
            statements.add(generateUndistributeTableStatement(getTableName() + parameters.getHistoryTableSuffix()));
        }

        return statements.toArray(new SqlStatement[0]);
    }

    protected DdmUndistributeTableStatement generateUndistributeTableStatement(String tableName) {
        return new DdmUndistributeTableStatement(tableName);
    }

    @DatabaseChangeProperty()
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @DatabaseChangeProperty()
    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @Override
    public String getConfirmationMessage() {
        return "Undistribute Table " + tableName + " created";
    }
}