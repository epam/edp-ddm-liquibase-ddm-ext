package com.epam.digital.data.platform.liquibase.extension.change.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.statement.SqlStatement;
import liquibase.util.JdbcUtils;

/**
 * Expose Search Condition.
 */
@DatabaseChange(name="exposeSearchCondition", description = "Expose Search Condition", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmExposeSearchConditionChange extends AbstractChange {

    private String name;
    private String consumer;

    private boolean existsInChangeLog() {
        boolean scExists = false;

        for (ChangeSet changeSet : this.getChangeSet().getChangeLog().getChangeSets()) {
            for (Change change: changeSet.getChanges()) {
                if (change instanceof DdmCreateSearchConditionChange) {
                    DdmCreateSearchConditionChange searchConditionChange = (DdmCreateSearchConditionChange) change;
                    if (searchConditionChange.getName().equals(getName())) {
                        scExists = true;
                        break;
                    }
                }
            }
        }

        return scExists;
    }

    private ValidationErrors validateViewExists(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        Statement statement = null;
        ResultSet resultSet = null;

        if ((!existsInChangeLog()) && (database.getConnection() instanceof JdbcConnection)) {
            try {
                statement = ((JdbcConnection) database.getConnection()).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

                String sql = "SELECT " + DdmConstants.METADATA_CHANGE_NAME + " FROM " + database.escapeTableName(null, null, DdmConstants.METADATA_TABLE) +
                    " WHERE " + DdmConstants.METADATA_CHANGE_TYPE + " = '" + DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE + "'" +
                    " AND " + DdmConstants.METADATA_CHANGE_NAME + " = '" + getName() + "';";

                resultSet = statement.executeQuery(sql);

                if (!resultSet.next()) {
                    validationErrors.addError("Search Condition '" + getName() + "' does not exist");
                }
            } catch (SQLException | DatabaseException e) {
                Scope.getCurrentScope().getLog(database.getClass()).info("Cannot check existing Search Condition '" + getName() + "'", e);
            } finally {
                JdbcUtils.close(resultSet, statement);
            }
        }

        return validationErrors;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));
        validationErrors.addAll(validateViewExists(database));

        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{
            DdmUtils.insertMetadata(DdmConstants.ATTRIBUTE_EXPOSE, getConsumer(), DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE, getName())
        };
    }

    @Override
    public String getConfirmationMessage() {
        return "Expose Search Condition " + getName();
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConsumer() {
        return consumer;
    }

    public void setConsumer(String consumer) {
        this.consumer = consumer;
    }
}