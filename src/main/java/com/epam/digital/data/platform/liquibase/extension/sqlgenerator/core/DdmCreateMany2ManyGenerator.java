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

import java.util.List;
import java.util.stream.Collectors;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateMany2ManyStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmCreateMany2ManyGenerator extends AbstractSqlGenerator<DdmCreateMany2ManyStatement> {

    @Override
    public ValidationErrors validate(DdmCreateMany2ManyStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("mainTableName", statement.getMainTableName());
        validationErrors.checkRequiredField("mainTableKeyField", statement.getMainTableKeyField());
        validationErrors.checkRequiredField("referenceTableName", statement.getReferenceTableName());
        validationErrors.checkRequiredField("referenceKeysArray", statement.getReferenceKeysArray());
        return validationErrors;
    }

    private String getListOfColumns(String tableAlias, List<DdmColumnConfig> columns) {
        return columns.stream().map(column -> tableAlias + "." + column.getNameAsAlias())
            .collect(Collectors.joining(", "));
    }

    private String getListOfAliases(List<DdmColumnConfig> columns) {
        return columns.stream()
            .map(column -> "main_cte" + "." + (column.getAlias() == null ? column.getName() : column.getAlias()))
            .collect(Collectors.joining(", "));
    }

    private StringBuilder getMainSql(DdmCreateMany2ManyStatement statement) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("SELECT ");
        buffer.append(statement.getMainTableName())
            .append(".")
            .append(statement.getMainTableKeyField());
        buffer.append(", UNNEST(")
            .append(statement.getMainTableName())
            .append(".")
            .append(statement.getReferenceKeysArray())
            .append(")");
        buffer.append(" AS ");
        buffer.append(statement.getReferenceColumnName());

        if (!statement.getMainTableColumns().isEmpty()) {
            buffer.append(", ");
            buffer.append(getListOfColumns(statement.getMainTableName(), statement.getMainTableColumns()));
        }

        buffer.append(" FROM ");
        buffer.append(statement.getMainTableName());

        return buffer;
    }

    private StringBuilder getTriggerSql(DdmCreateMany2ManyStatement statement) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("CREATE TRIGGER ");
        buffer.append("trg_")
            .append(statement.getReferenceTableName())
            .append("_integrity_")
            .append(statement.getMainTableName())
            .append("_")
            .append(statement.getReferenceKeysArray());
        buffer.append(" BEFORE UPDATE OR DELETE ON ")
            .append(statement.getReferenceTableName());
        buffer.append(" FOR EACH ROW");
        buffer.append(" EXECUTE FUNCTION f_trg_check_m2m_integrity('")
            .append(statement.getReferenceColumnName())
            .append("', '")
            .append(statement.getMainTableName())
            .append("', '")
            .append(statement.getReferenceKeysArray())
            .append("');");

        return buffer;
    }

    private StringBuilder getIndexSql(DdmCreateMany2ManyStatement statement) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("CREATE INDEX ");
        buffer.append(DdmConstants.PREFIX_INDEX)
            .append(statement.getName())
            .append(DdmConstants.SUFFIX_M2M);
        buffer.append(" ON ");
        buffer.append(statement.getMainTableName());
        buffer.append(" USING gin(")
            .append(statement.getReferenceKeysArray())
            .append(");");

        return buffer;
    }

    @Override
    public Sql[] generateSql(DdmCreateMany2ManyStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("CREATE OR REPLACE VIEW ");
        buffer.append(statement.getViewName());
        buffer.append(" AS ");

        if (!statement.getReferenceTableColumns().isEmpty()) {
            buffer.append("WITH main_cte as (");
            buffer.append(getMainSql(statement));
            buffer.append(") ");
            buffer.append("SELECT ");
            buffer.append("main_cte.")
                .append(statement.getMainTableKeyField())
                .append(", ");
            buffer.append("main_cte.")
                .append(statement.getReferenceColumnName());

            if (!statement.getMainTableColumns().isEmpty()) {
                buffer.append(", ");
                buffer.append(getListOfAliases(statement.getMainTableColumns()));
            }

            buffer.append(", ");
            buffer.append(getListOfColumns(statement.getReferenceTableName(), statement.getReferenceTableColumns()));
            buffer.append(" FROM main_cte");
            buffer.append(" JOIN ").append(statement.getReferenceTableName());
            buffer.append(" USING (");
            buffer.append(statement.getReferenceColumnName());
            buffer.append(")");
        } else {
            buffer.append(getMainSql(statement));
        }

        buffer.append(";");

        buffer.append("\n\n");
        buffer.append(getIndexSql(statement));

        buffer.append("\n\n");
        buffer.append(getTriggerSql(statement));

        return new Sql[]{ new UnparsedSql(buffer.toString()) };
    }

}
