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
