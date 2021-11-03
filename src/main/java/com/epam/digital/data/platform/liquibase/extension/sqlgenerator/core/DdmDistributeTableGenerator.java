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
