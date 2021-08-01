package liquibase.sqlgenerator.core;

import liquibase.change.DdmColumnConfig;
import liquibase.change.DdmLabelConfig;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DdmCreateTypeStatement;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.isNull;

public class DdmCreateTypeGenerator extends AbstractSqlGenerator<DdmCreateTypeStatement> {

    @Override
    public ValidationErrors validate(DdmCreateTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", statement.getName());
        if (statement.getAsEnum() != null) {
            validationErrors.checkRequiredField("labels", statement.getAsEnum().getLabels());
        } else if (statement.getAsComposite() != null) {
            validationErrors.checkRequiredField("columns", statement.getAsComposite().getColumns());
        }        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmCreateTypeStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("CREATE TYPE ");
        buffer.append(statement.getName());

        if (!isNull(statement.getAsEnum())) {
            List<String> labels = new ArrayList<>();

            buffer.append(" as ENUM (");

            for (DdmLabelConfig label : statement.getAsEnum().getLabels()) {
                labels.add("'" + label.getLabel() + "'");
            }

            buffer.append(String.join(", ", labels)).append(");");
        } else if (!isNull(statement.getAsComposite())) {
            List<String> columns = new ArrayList<>();
            buffer.append(" as (");

            for (DdmColumnConfig column : statement.getAsComposite().getColumns()) {
                columns.add(column.getName() + " " + column.getType() +
                        ((!isNull(column.getCollation())) ? " COLLATE \"" + column.getCollation() + "\"" : ""));
            }

            buffer.append(String.join(", ", columns)).append(");");
        }

        return new Sql[] {
                new UnparsedSql(buffer.toString())
        };
    }
}
