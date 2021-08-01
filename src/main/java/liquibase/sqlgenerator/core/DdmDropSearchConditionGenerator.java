package liquibase.sqlgenerator.core;

import liquibase.DdmConstants;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DdmDropSearchConditionStatement;

public class DdmDropSearchConditionGenerator extends AbstractSqlGenerator<DdmDropSearchConditionStatement> {

    @Override
    public ValidationErrors validate(DdmDropSearchConditionStatement ddmDropSearchConditionStatement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", ddmDropSearchConditionStatement.getName());
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(DdmDropSearchConditionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("drop view if exists ").append(statement.getName()).append(DdmConstants.SUFFIX_VIEW).append(";");
        buffer.append("\n\n");
        buffer.append("delete from ").append(DdmConstants.METADATA_TABLE);
        buffer.append(" where (").append(DdmConstants.METADATA_CHANGE_TYPE).append(" = '").append(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE).append("') and (");
        buffer.append(DdmConstants.METADATA_CHANGE_NAME).append(" = '").append(statement.getName()).append("');");
        buffer.append("\n\n");
        buffer.append("do $$");
        buffer.append("  declare");
        buffer.append("    txt text; ");
        buffer.append("begin");
        buffer.append("  select");
        buffer.append("    string_agg('drop index if exists ' || indexname, '; ') || ';'");
        buffer.append("  into txt");
        buffer.append("  from pg_indexes");
        buffer.append("  where indexname like '").append(DdmConstants.PREFIX_INDEX).append("$").append(statement.getName()).append("$_%'; ");
        buffer.append("  if txt is not null then");
        buffer.append("    execute txt;");
        buffer.append("  end if; ");
        buffer.append("end; $$");

        return new Sql[]{
            new UnparsedSql(buffer.toString())
        };
    }

 }
