package liquibase.change.core;

import liquibase.DdmParameters;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DdmReferenceTableStatement;

import java.util.ArrayList;
import java.util.List;

import static liquibase.DdmParameters.*;

/**
 * Creates a new Reference Table.
 */
@DatabaseChange(name="referenceTable", description = "Reference Table", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmReferenceTableChange extends AbstractChange {
    private String tableName;
    private String scope;

    public DdmReferenceTableChange() {
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
            statements.add(generateReferenceTableStatement(getTableName()));
        }

        if (!isNull(getScope()) && (isAll(getScope()) || isHistory(getScope()))) {
            DdmParameters parameters = new DdmParameters();
            statements.add(generateReferenceTableStatement(getTableName() + parameters.getHistoryTableSuffix()));
        }

        return statements.toArray(new SqlStatement[statements.size()]);
    }

    protected DdmReferenceTableStatement generateReferenceTableStatement(String tableName) {
        return new DdmReferenceTableStatement(tableName);
    }

    @DatabaseChangeProperty()
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @Override
    public String getConfirmationMessage() {
        return "Reference Table " + tableName + " created";
    }
}