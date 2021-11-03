package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmUndistributeTableStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmUndistributeTableGenerator extends AbstractSqlGenerator<DdmUndistributeTableStatement> {

    @Override
    public ValidationErrors validate(DdmUndistributeTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", statement.getTableName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmUndistributeTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String buffer = "SELECT undistribute_table('"
            + statement.getTableName() + "')";

        return new Sql[]{ new UnparsedSql(buffer) };
    }

}
