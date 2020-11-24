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

package com.epam.digital.data.platform.liquibase.extension.statement.core;

import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmDistributeTableStatement extends AbstractSqlStatement implements CompoundStatement {
    private final String tableName;
    private final String distributionColumn;
    private String distributionType;
    private String colocateWith;

    public DdmDistributeTableStatement(String tableName, String distributionColumn) {
        this.tableName = tableName;
        this.distributionColumn = distributionColumn;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDistributionColumn() {
        return distributionColumn;
    }

    public String getDistributionType() {
        return distributionType;
    }

    public void setDistributionType(String distributionType) {
        this.distributionType = distributionType;
    }

    public String getColocateWith() {
        return colocateWith;
    }

    public void setColocateWith(String colocateWith) {
        this.colocateWith = colocateWith;
    }
}
