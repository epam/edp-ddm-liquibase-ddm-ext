package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DdmReferenceTableStatement;

public class DdmReferenceTableGenerator extends AbstractSqlGenerator<DdmReferenceTableStatement> {

    @Override
    public ValidationErrors validate(DdmReferenceTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", statement.getTableName());

        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmReferenceTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        StringBuilder buffer = new StringBuilder();
        buffer.append("SELECT create_reference_table('");
        buffer.append(statement.getTableName());
        buffer.append("')");

        return new Sql[]{
                new UnparsedSql(buffer.toString())
        };
    }

}
