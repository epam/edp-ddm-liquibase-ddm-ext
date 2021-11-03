package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDropTypeStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmDropTypeGenerator extends AbstractSqlGenerator<DdmDropTypeStatement> {

    @Override
    public ValidationErrors validate(DdmDropTypeStatement ddmDropTypeStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("typeName", ddmDropTypeStatement.getName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmDropTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[]{ new UnparsedSql("DROP TYPE " + statement.getName() + ";") };
    }

 }
