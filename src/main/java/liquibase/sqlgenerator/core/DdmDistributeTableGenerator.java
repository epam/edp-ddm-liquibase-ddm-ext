package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DdmDistributeTableStatement;

import static liquibase.DdmParameters.isNull;

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

        if (!isNull(statement.getDistributionType())) {
            buffer.append(", '");
            buffer.append(statement.getDistributionType());
            buffer.append("'");
        }

        if (!isNull(statement.getColocateWith())) {
            buffer.append(", colocate_with=>'");
            buffer.append(statement.getColocateWith());
            buffer.append("'");
        }

        buffer.append(")");

        return new Sql[]{
                new UnparsedSql(buffer.toString())
        };
    }
}
