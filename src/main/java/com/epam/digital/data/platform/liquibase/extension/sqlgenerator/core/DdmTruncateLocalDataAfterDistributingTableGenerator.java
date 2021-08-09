package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmTruncateLocalDataAfterDistributingTableStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmTruncateLocalDataAfterDistributingTableGenerator extends AbstractSqlGenerator<DdmTruncateLocalDataAfterDistributingTableStatement> {

    @Override
    public ValidationErrors validate(DdmTruncateLocalDataAfterDistributingTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", statement.getTableName());

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmTruncateLocalDataAfterDistributingTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        StringBuilder buffer = new StringBuilder();
        buffer.append("SELECT truncate_local_data_after_distributing_table('");
        buffer.append(statement.getTableName());
        buffer.append("')");

        return new Sql[]{
                new UnparsedSql(buffer.toString())
        };
    }

}
