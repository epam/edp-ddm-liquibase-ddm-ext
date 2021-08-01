package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DdmDropDomainStatement;

public class DdmDropDomainGenerator extends AbstractSqlGenerator<DdmDropDomainStatement> {

    @Override
    public ValidationErrors validate(DdmDropDomainStatement ddmDropDomainStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", ddmDropDomainStatement.getName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmDropDomainStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("DROP DOMAIN ").append(statement.getName()).append(";");

        return new Sql[]{
            new UnparsedSql(buffer.toString())
        };
    }

 }
