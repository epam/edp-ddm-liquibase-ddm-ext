package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateMany2ManyStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmCreateMany2ManyGenerator extends AbstractSqlGenerator<DdmCreateMany2ManyStatement> {

    @Override
    public ValidationErrors validate(DdmCreateMany2ManyStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("mainTableName", statement.getMainTableName());
        validationErrors.checkRequiredField("mainTableKeyField", statement.getMainTableKeyField());
        validationErrors.checkRequiredField("referenceTableName", statement.getReferenceTableName());
        validationErrors.checkRequiredField("referenceKeysArray", statement.getReferenceKeysArray());
        return validationErrors;
    }

    private String getListOfColumns(String tableAlias, List<DdmColumnConfig> columns) {
        List<String> listColumns = new ArrayList<>();
        for (DdmColumnConfig column : columns) {
            listColumns.add(tableAlias + "." + column.getNameAsAlias());
        }

        return String.join(", ", listColumns);
    }

    private String getListOfAliases(String tableAlias, List<DdmColumnConfig> columns) {
        List<String> listColumns = new ArrayList<>();
        for (DdmColumnConfig column : columns) {
            listColumns.add(tableAlias + "." + (Objects.isNull(column.getAlias()) ? column.getName() : column.getAlias()));
        }

        return String.join(", ", listColumns);
    }

    private StringBuilder getMainSql(DdmCreateMany2ManyStatement statement) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("SELECT ");
        buffer.append(statement.getMainTableName() + "." + statement.getMainTableKeyField());
        buffer.append(", UNNEST(" + statement.getMainTableName() + "." + statement.getReferenceKeysArray() + ")");
        buffer.append(" AS ");
        buffer.append(statement.getReferenceTableName() + DdmConstants.SUFFIX_ID);

        if (!statement.getMainTableColumns().isEmpty()) {
            buffer.append(", ");
            buffer.append(getListOfColumns(statement.getMainTableName(), statement.getMainTableColumns()));
        }

        buffer.append(" FROM ");
        buffer.append(statement.getMainTableName());

        return buffer;
    }

    @Override
    public Sql[] generateSql(DdmCreateMany2ManyStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("CREATE OR REPLACE VIEW ");
        buffer.append(statement.getViewName());
        buffer.append(" AS ");

        if (!statement.getReferenceTableColumns().isEmpty()) {
            buffer.append("WITH main_cte as (");
            buffer.append(getMainSql(statement));
            buffer.append(") ");
            buffer.append("SELECT ");
            buffer.append("main_cte." + statement.getMainTableKeyField() + ", ");
            buffer.append("main_cte." + statement.getReferenceTableName() + DdmConstants.SUFFIX_ID);

            if (!statement.getMainTableColumns().isEmpty()) {
                buffer.append(", ");
                buffer.append(getListOfAliases("main_cte", statement.getMainTableColumns()));
            }

            buffer.append(", ");
            buffer.append(getListOfColumns(statement.getReferenceTableName(), statement.getReferenceTableColumns()));
            buffer.append(" FROM main_cte");
            buffer.append(" JOIN ").append(statement.getReferenceTableName());
            buffer.append(" USING (");
            buffer.append(statement.getReferenceTableName() + DdmConstants.SUFFIX_ID);
            buffer.append(")");
        } else {
            buffer.append(getMainSql(statement));
        }

        buffer.append(";");

        buffer.append("\n\n");

        buffer.append("CREATE INDEX ");
        buffer.append(DdmConstants.PREFIX_INDEX);
        buffer.append(statement.getName());
        buffer.append(DdmConstants.SUFFIX_M2M);
        buffer.append(" ON ");
        buffer.append(statement.getMainTableName());
        buffer.append(" USING gin(");
        buffer.append(statement.getReferenceKeysArray());
        buffer.append(");");

        return new Sql[] {
            new UnparsedSql(buffer.toString())
        };
    }

}
