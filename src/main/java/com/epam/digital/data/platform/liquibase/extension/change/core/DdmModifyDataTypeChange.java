package com.epam.digital.data.platform.liquibase.extension.change.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.epam.digital.data.platform.liquibase.extension.DdmParameters;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.ModifyDataTypeChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.ModifyDataTypeStatement;

@DatabaseChange(
    name="modifyDataType",
    description = "Modify data type",
    priority = ChangeMetaData.PRIORITY_DEFAULT + 50,
    appliesTo = "column")
public class DdmModifyDataTypeChange extends ModifyDataTypeChange {

    private Boolean historyFlag;
    private DdmParameters parameters = new DdmParameters();

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        statements.add(new ModifyDataTypeStatement(getCatalogName(), getSchemaName(), getTableName(), getColumnName(), getNewDataType()));

        if (Objects.nonNull(getHistoryFlag()) && getHistoryFlag()) {
            String historyTableName = getTableName() + parameters.getHistoryTableSuffix();
            statements.add(new ModifyDataTypeStatement(getCatalogName(), getSchemaName(), historyTableName, getColumnName(), getNewDataType()));
        }

        return statements.toArray(new SqlStatement[statements.size()]);
    }

    public Boolean getHistoryFlag() {
        return historyFlag;
    }

    public void setHistoryFlag(Boolean historyFlag) {
        this.historyFlag = historyFlag;
    }
}
