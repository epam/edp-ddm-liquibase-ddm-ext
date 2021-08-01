package liquibase.change.core;

import liquibase.DdmParameters;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DdmTruncateLocalDataAfterDistributingTableStatement;

import java.util.ArrayList;
import java.util.List;

import static liquibase.DdmParameters.*;

/**
 * Creates a new Truncate Local Data After Distributing Table.
 */
@DatabaseChange(name="truncateLocalDataAfterDistributingTable", description = "Truncate Local Data After Distributing Table", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmTruncateLocalDataAfterDistributingTableChange extends AbstractChange {
    private String tableName;
    private String scope;

    public DdmTruncateLocalDataAfterDistributingTableChange() {
        super();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        if (isNull(getScope())
                || isEmpty(getScope())
                || isAll(getScope())
                || isPrimary(getScope())) {
            statements.add(generateTruncateLocalDataAfterDistributingTableStatement(getTableName()));
        }

        if (!isNull(getScope()) && (isAll(getScope()) || isHistory(getScope()))) {
            DdmParameters parameters = new DdmParameters();
            statements.add(generateTruncateLocalDataAfterDistributingTableStatement(getTableName() + parameters.getHistoryTableSuffix()));
        }

        return statements.toArray(new SqlStatement[statements.size()]);
    }

    protected DdmTruncateLocalDataAfterDistributingTableStatement generateTruncateLocalDataAfterDistributingTableStatement(String tableName) {
        return new DdmTruncateLocalDataAfterDistributingTableStatement(tableName);
    }

    @DatabaseChangeProperty()
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @DatabaseChangeProperty()
    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @Override
    public String getConfirmationMessage() {
        return "Truncate Local Data After Distributing Table " + tableName + " created";
    }
}