package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DdmDropTypeStatement;

public class DdmDropTypeGenerator extends AbstractSqlGenerator<DdmDropTypeStatement> {

    @Override
    public ValidationErrors validate(DdmDropTypeStatement ddmDropTypeStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("typeName", ddmDropTypeStatement.getName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmDropTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("DROP TYPE ").append(statement.getName()).append(";");

        return new Sql[]{
            new UnparsedSql(buffer.toString())
        };
    }

 }
