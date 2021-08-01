package liquibase.sqlgenerator.core;

import liquibase.change.DdmDomainConstraintConfig;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DdmCreateDomainStatement;

import java.util.ArrayList;
import java.util.List;

import static liquibase.DdmParameters.isNull;

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

        List<Sql> additionalSql = new ArrayList<>();

        StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE DOMAIN ");
        buffer.append(statement.getName());
        buffer.append(" AS ");
        buffer.append(statement.getType());

        if (!isNull(statement.getNullable()) && (!statement.getNullable())) {
            buffer.append(" NOT NULL");
        }

        if (!isNull(statement.getCollation())) {
            buffer.append(" COLLATE \"").append(statement.getCollation()).append("\"");
        }


        if (!isNull(statement.getDefaultValue())) {
            buffer.append(" DEFAULT ").append(statement.getDefaultValue());
        }

        if (statement.getConstraints() != null) {
            for (DdmDomainConstraintConfig config : statement.getConstraints()) {
                if (!isNull(config.getName())) {
                    buffer.append(" CONSTRAINT ").append(config.getName());
                }
                buffer.append(" CHECK (").append(config.getImplementation()).append(")");
            }
        }

        buffer.append(";");

        return new Sql[]{
                new UnparsedSql(buffer.toString())
        };
    }
}
