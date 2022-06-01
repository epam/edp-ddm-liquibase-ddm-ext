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

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSimpleSearchConditionStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmCreateSimpleSearchConditionGenerator extends AbstractSqlGenerator<DdmCreateSimpleSearchConditionStatement> {

    @Override
    public ValidationErrors validate(DdmCreateSimpleSearchConditionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", statement.getName());
        return validationErrors;
    }

    private StringBuilder generateIndexSql(DdmCreateSimpleSearchConditionStatement statement, String searchType, boolean changeName) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("CREATE INDEX ");
        buffer.append(DdmConstants.PREFIX_INDEX);
        buffer.append(statement.getName());
        buffer.append("_");
        buffer.append(statement.getTable().getName());
        buffer.append("_");
        buffer.append(statement.getSearchColumn().getName());

        if (changeName) {
            buffer.append("_");
            buffer.append(searchType);
        }

        buffer.append(" ON ");
        buffer.append(statement.getTable().getName());
        buffer.append("(");
        boolean isColumnCastable = DdmUtils.isColumnAvailableForCasting(statement.getSearchColumn());
        if (isColumnCastable) {
            buffer.append("lower(cast(");
        }
        buffer.append(statement.getSearchColumn().getName());
        if (isColumnCastable) {
            buffer.append(" as varchar))");
        }

        if (searchType.equalsIgnoreCase(DdmConstants.ATTRIBUTE_CONTAINS) || searchType.equalsIgnoreCase(DdmConstants.ATTRIBUTE_STARTS_WITH)) {
            buffer.append(" ");

            if (statement.getSearchColumn().getType().equalsIgnoreCase(DdmConstants.TYPE_CHAR)) {
                buffer.append("bp");
            }

            buffer.append(statement.getSearchColumn().getType().toLowerCase());
            buffer.append("_pattern_ops");
        }

        buffer.append(");");

        return buffer;
    }

    @Override
    public Sql[] generateSql(DdmCreateSimpleSearchConditionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("CREATE OR REPLACE VIEW ");
        buffer.append(statement.getName());
        buffer.append(DdmConstants.SUFFIX_VIEW);
        buffer.append(" AS ");
        buffer.append("SELECT ");
        buffer.append(statement.getTable().getAlias());
        buffer.append(".* ");
        buffer.append("FROM ");
        buffer.append(statement.getTable().getName());
        buffer.append(" AS ");
        buffer.append(statement.getTable().getAlias());

        buffer.append(";");

        if (Boolean.TRUE.equals(statement.getIndexing())) {
            buffer.append("\n\n");
            buffer.append(generateIndexSql(statement, statement.getSearchColumn().getSearchType(), false));
        }

        return new Sql[] { new UnparsedSql(buffer.toString()) };
    }

}
