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

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateTypeStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

import java.util.stream.Collectors;

public class DdmCreateTypeGenerator extends AbstractSqlGenerator<DdmCreateTypeStatement> {

    @Override
    public ValidationErrors validate(DdmCreateTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", statement.getName());
        if (statement.getAsEnum() != null) {
            validationErrors.checkRequiredField("labels", statement.getAsEnum().getLabels());
        } else if (statement.getAsComposite() != null) {
            validationErrors.checkRequiredField("columns", statement.getAsComposite().getColumns());
        }
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmCreateTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE TYPE ");
        buffer.append(statement.getName());

        if (statement.getAsEnum() != null) {
            buffer.append(" as ENUM (");

            String labels = statement.getAsEnum().getLabels().stream().map(label -> "'" + label.getLabel() + "'")
                .collect(Collectors.joining(", "));

            buffer.append(labels).append(");");
        } else if (statement.getAsComposite() != null) {
            buffer.append(" as (");

            String columns = statement.getAsComposite().getColumns().stream()
                .map(column -> column.getName() + " " + column.getType() +
                    ((column.getCollation() != null) ? " COLLATE \"" + column.getCollation() + "\"" : ""))
                .collect(Collectors.joining(", "));

            buffer.append(columns).append(");");
        }

        return new Sql[]{ new UnparsedSql(buffer.toString()) };
    }
}
