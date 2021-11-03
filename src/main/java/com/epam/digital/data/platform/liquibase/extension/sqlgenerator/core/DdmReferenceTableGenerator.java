package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmReferenceTableStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmReferenceTableGenerator extends AbstractSqlGenerator<DdmReferenceTableStatement> {

    @Override
    public ValidationErrors validate(DdmReferenceTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", statement.getTableName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmReferenceTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String buffer = "SELECT create_reference_table('"
            + statement.getTableName() + "')";

        return new Sql[]{ new UnparsedSql(buffer) };
    }

}
