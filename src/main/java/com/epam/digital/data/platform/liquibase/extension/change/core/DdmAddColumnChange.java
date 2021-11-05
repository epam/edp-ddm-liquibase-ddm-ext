package com.epam.digital.data.platform.liquibase.extension.change.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmParameters;
import liquibase.Scope;
import liquibase.change.AddColumnConfig;
import liquibase.change.ChangeMetaData;
import liquibase.change.ColumnConfig;
import liquibase.change.ConstraintsConfig;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddColumnChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.LiquibaseDataType;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DropUniqueConstraintStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;
import liquibase.structure.core.UniqueConstraint;
import liquibase.util.JdbcUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Adds a column to an existing table with history.
 */
@DatabaseChange(name="addColumn", description = "Adds a new column to an existing table with history", priority = ChangeMetaData.PRIORITY_DEFAULT + 50, appliesTo = "table")
public class DdmAddColumnChange extends AddColumnChange {

    private Boolean historyFlag;
    private final DdmParameters parameters = new DdmParameters();
    private boolean isHistoryTable;
    private final SnapshotGeneratorFactory snapshotGeneratorFactory;

    public DdmAddColumnChange() {
        this(SnapshotGeneratorFactory.getInstance());
    }

    DdmAddColumnChange(SnapshotGeneratorFactory instance) {
        snapshotGeneratorFactory = instance;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        for (AddColumnConfig column : getColumns()) {
            if (column.getDefaultValueObject() == null
                && column.getConstraints() != null
                && !column.getConstraints().isNullable()) {
                validationErrors.addError("Please set default value to not nullable column " + column.getName());
            }
        }

        if (getVersion(database) == null) {
            validationErrors.addError("Cannot select version!");
        }
        return validationErrors;
    }

    private String getVersion(Database database) {
        String version = null;

        Statement statement = null;
        ResultSet resultSet = null;

        if (database.getConnection() instanceof JdbcConnection) {
            try {
                statement = ((JdbcConnection) database.getConnection()).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

                String sql = "SELECT " + DdmConstants.METADATA_ATTRIBUTE_VALUE + " FROM " + DdmConstants.METADATA_TABLE +
                    " WHERE " + DdmConstants.METADATA_CHANGE_TYPE + " = '" + DdmConstants.VERSIONING_METADATA_CHANGE_TYPE_VALUE + "'" +
                    " AND " + DdmConstants.METADATA_CHANGE_NAME + " = '" + DdmConstants.VERSIONING_METADATA_CHANGE_NAME_VALUE + "'" +
                    " AND " + DdmConstants.METADATA_ATTRIBUTE_NAME + " = '" + DdmConstants.VERSIONING_METADATA_ATTRIBUTE_NAME_CURRENT + "'";

                resultSet = statement.executeQuery(sql);

                if (resultSet.next()) {
                    version = resultSet.getString(DdmConstants.METADATA_ATTRIBUTE_VALUE);
                    version = "_" + version.replace(".", "_");
                }
            } catch (SQLException | DatabaseException e) {
                Scope.getCurrentScope().getLog(database.getClass()).info("Cannot select version", e);
            } finally {
                JdbcUtils.close(resultSet, statement);
            }
        }

        return version;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>(Arrays.asList(super.generateStatements(database)));

        if (Boolean.TRUE.equals(historyFlag)) {
            isHistoryTable = true;
            String newTableName = getTableName() + getVersion(database);

            Table snapshotTable = null;
            try {
                snapshotTable = snapshotGeneratorFactory.createSnapshot(new Table(getCatalogName(), getSchemaName(), getTableName()), database);
            } catch (Exception e) {
                Scope.getCurrentScope().getLog(database.getClass()).info("Cannot generate snapshot of table " + getTableName() + " on " + database.getShortName() + " database", e);
            }

            if (snapshotTable != null) {
                for (UniqueConstraint uniqueConstraint : snapshotTable.getUniqueConstraints()) {
                    statements.add(new DropUniqueConstraintStatement(null, null, snapshotTable.getName(), uniqueConstraint.getName()));
                }
            }

            statements.add(new RenameTableStatement(null, null, getTableName(), newTableName));

            if (snapshotTable != null) {
                CreateTableStatement statement = new CreateTableStatement(null, null, snapshotTable.getName(), snapshotTable.getRemarks());

                for (Column column : snapshotTable.getColumns()) {
                    LiquibaseDataType columnType = DataTypeFactory.getInstance().fromDescription(column.getType().getTypeName(), database);
                    statement.addColumn(column.getName(), columnType, column.getDefaultValueConstraintName(), column.getDefaultValue(), column.getRemarks());

                    if (!column.isNullable()) {
                        NotNullConstraint notNullConstraint = new NotNullConstraint(column.getName());
                        notNullConstraint.setValidateNullable(column.getValidateNullable());
                        statement.addColumnConstraint(notNullConstraint);
                    }
                }

                for (ColumnConfig column : getColumns()) {
                    LiquibaseDataType columnType = DataTypeFactory.getInstance().fromDescription(column.getType(), database);
                    statement.addColumn(column.getName(), columnType, column.getDefaultValueConstraintName(), column.getDefaultValue(), column.getRemarks());

                    ConstraintsConfig constraints = column.getConstraints();
                    if (constraints != null && constraints.isNullable() != null && !constraints.isNullable()) {
                        NotNullConstraint notNullConstraint = new NotNullConstraint(column.getName());
                        notNullConstraint.setConstraintName(constraints.getNotNullConstraintName());
                        notNullConstraint.setValidateNullable(constraints.getValidateNullable() == null || constraints.getValidateNullable());
                        statement.addColumnConstraint(notNullConstraint);
                    }
                }

                statement.getUniqueConstraints().clear();
                for (UniqueConstraint uniqueConstraint : snapshotTable.getUniqueConstraints()) {
                    liquibase.statement.UniqueConstraint uc = new liquibase.statement.UniqueConstraint(uniqueConstraint.getName());
                    uniqueConstraint.getColumns().stream().map(Column::getName).forEach(uc::addColumns);
                    statement.addColumnConstraint(uc);
                }

                statements.add(statement);
                statements.add(new RawSqlStatement("CALL p_init_new_hist_table('" + newTableName + "', '" + snapshotTable.getName() + "');"));
                statements.add(new RawSqlStatement("ALTER TABLE " + newTableName + " SET SCHEMA archive;"));
            }

            isHistoryTable = false;
        }

        return statements.toArray(new SqlStatement[0]);
    }

    public Boolean getHistoryFlag() {
        return historyFlag;
    }

    public void setHistoryFlag(Boolean historyFlag) {
        this.historyFlag = historyFlag;
    }

    @Override
    public String getTableName() {
        if (Boolean.TRUE.equals(historyFlag)) {
            return super.getTableName() + (isHistoryTable ? parameters.getHistoryTableSuffix() : "");
        }
        return super.getTableName();
    }
}
