package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmParameters;
import liquibase.change.AbstractChange;
import liquibase.change.DatabaseChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChangeProperty;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDistributeTableStatement;
import liquibase.util.StringUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new Distribute Table.
 */
@DatabaseChange(name="distributeTable", description = "Distribute Table", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmDistributeTableChange extends AbstractChange {
    private String tableName;
    private String distributionColumn;
    private String distributionType;
    private String colocateWith;
    private String scope;

    public DdmDistributeTableChange() {
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

        if (StringUtil.isEmpty(getScope())
                || DdmParameters.isAll(getScope())
                || DdmParameters.isPrimary(getScope())) {
            statements.add(generateDistributeTableStatement(getTableName()));
        }

        if (getScope() != null && (DdmParameters.isAll(getScope()) || DdmParameters.isHistory(getScope()))) {
            DdmParameters parameters = new DdmParameters();
            statements.add(generateDistributeTableStatement(getTableName() + parameters.getHistoryTableSuffix()));
        }

        return statements.toArray(new SqlStatement[0]);
    }

    protected DdmDistributeTableStatement generateDistributeTableStatement(String tableName) {
        DdmDistributeTableStatement statement = new DdmDistributeTableStatement(tableName, getDistributionColumn());
        statement.setDistributionType(getDistributionType());
        statement.setColocateWith(getColocateWith());
        return statement;
    }

    @DatabaseChangeProperty()
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @DatabaseChangeProperty()
    public String getDistributionColumn() {
        return distributionColumn;
    }

    public void setDistributionColumn(String distributionColumn) {
        this.distributionColumn = distributionColumn;
    }

    @DatabaseChangeProperty()
    public String getDistributionType() {
        return distributionType;
    }

    public void setDistributionType(String distributionType) {
        this.distributionType = distributionType;
    }

    @DatabaseChangeProperty()
    public String getColocateWith() {
        return colocateWith;
    }

    public void setColocateWith(String colocateWith) {
        this.colocateWith = colocateWith;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @Override
    public String getConfirmationMessage() {
        return "Distribute Table " + tableName + " created";
    }
}