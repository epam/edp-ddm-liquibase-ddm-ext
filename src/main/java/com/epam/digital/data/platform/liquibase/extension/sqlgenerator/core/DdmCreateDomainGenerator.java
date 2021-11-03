package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmDomainConstraintConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateDomainStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmCreateDomainGenerator extends AbstractSqlGenerator<DdmCreateDomainStatement> {

    @Override
    public ValidationErrors validate(DdmCreateDomainStatement ddmCreateDomainStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", ddmCreateDomainStatement.getName());
        validationErrors.checkRequiredField("type", ddmCreateDomainStatement.getType());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmCreateDomainStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE DOMAIN ");
        buffer.append(statement.getName());
        buffer.append(" AS ");
        buffer.append(statement.getType());

        if (Boolean.FALSE.equals(statement.getNullable())) {
            buffer.append(" NOT NULL");
        }

        if (statement.getCollation() != null) {
            buffer.append(" COLLATE \"").append(statement.getCollation()).append("\"");
        }


        if (statement.getDefaultValue() != null) {
            buffer.append(" DEFAULT ").append(statement.getDefaultValue());
        }

        if (statement.getConstraints() != null) {
            for (DdmDomainConstraintConfig config : statement.getConstraints()) {
                if (config.getName() != null) {
                    buffer.append(" CONSTRAINT ").append(config.getName());
                }
                buffer.append(" CHECK (").append(config.getImplementation()).append(")");
            }
        }

        buffer.append(";");

        return new Sql[]{ new UnparsedSql(buffer.toString()) };
    }
}
