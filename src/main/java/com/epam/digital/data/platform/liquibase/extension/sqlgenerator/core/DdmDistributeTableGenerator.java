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

package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDistributeTableStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmDistributeTableGenerator extends AbstractSqlGenerator<DdmDistributeTableStatement> {

    @Override
    public ValidationErrors validate(DdmDistributeTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", statement.getTableName());
        validationErrors.checkRequiredField("distributionColumn", statement.getDistributionColumn());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmDistributeTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        StringBuilder buffer = new StringBuilder();
        buffer.append("SELECT create_distributed_table('");
        buffer.append(statement.getTableName());
        buffer.append("', '");
        buffer.append(statement.getDistributionColumn());
        buffer.append("'");

        if (statement.getDistributionType() != null) {
            buffer.append(", '");
            buffer.append(statement.getDistributionType());
            buffer.append("'");
        }

        if (statement.getColocateWith() != null) {
            buffer.append(", colocate_with=>'");
            buffer.append(statement.getColocateWith());
            buffer.append("'");
        }

        buffer.append(")");

        return new Sql[]{ new UnparsedSql(buffer.toString()) };
    }
}
