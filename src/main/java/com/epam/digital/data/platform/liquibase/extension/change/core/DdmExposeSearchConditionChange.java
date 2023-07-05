/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.liquibase.extension.change.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
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
    private Boolean trembita;
    private Boolean platform;
    private Boolean externalSystem;
    private Boolean publicAccess;

    private boolean existsInChangeLog() {
        boolean scExists = false;
        return this.getChangeSet().getChangeLog().getChangeSets().stream().anyMatch(
            changeSet -> changeSet.getChanges().stream()
                .filter(change -> change instanceof DdmCreateSearchConditionChange)
                .map(change -> (DdmCreateSearchConditionChange) change)
                .anyMatch(searchConditionChange -> searchConditionChange.getName().equals(getName()))) || scExists;
    }

    private ValidationErrors validateViewExists(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        Statement statement = null;
        ResultSet resultSet = null;

        if ((!existsInChangeLog()) && (database.getConnection() instanceof JdbcConnection)) {
            try {
                statement = ((JdbcConnection) database.getConnection()).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

                String sql = "SELECT " + DdmConstants.METADATA_CHANGE_NAME + " FROM " + DdmConstants.METADATA_TABLE +
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
        if (!DdmUtils.isSearchConditionChangeSet(this.getChangeSet())){
            validationErrors.addError(DdmUtils.printConsistencyChangeSetError(getChangeSet().getId()));
        }
        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        if (DdmUtils.hasSubContext(this.getChangeSet())){
            this.getChangeSet().setIgnore(true);
            return new SqlStatement[0];
        }
        List<SqlStatement> statements = new ArrayList<>();
        if (Boolean.TRUE.equals(getTrembita())) {
            statements.add(insertMetadataForExposedSc(DdmConstants.ATTRIBUTE_EXPOSE_TREMBITA));
        }
        if (Boolean.TRUE.equals(getPlatform())) {
            statements.add(insertMetadataForExposedSc(DdmConstants.ATTRIBUTE_EXPOSE_PLATFORM));
        }
        if (Boolean.TRUE.equals(getExternalSystem())) {
            statements.add(insertMetadataForExposedSc(DdmConstants.ATTRIBUTE_EXPOSE_EXTERNAL_SYSTEM));
        }
        if (Boolean.TRUE.equals(getPublicAccess())) {
            statements.add(insertMetadataForExposedSc(DdmConstants.ATTRIBUTE_EXPOSE_PUBLIC_ACCESS));
        } else if (Boolean.FALSE.equals(getPublicAccess())) {
            statements.add(deleteMetadataForExposedSc(DdmConstants.ATTRIBUTE_EXPOSE_PUBLIC_ACCESS));
        }
        return statements.toArray(new SqlStatement[0]);
    }

    private SqlStatement insertMetadataForExposedSc(String consumer) {
        return DdmUtils.insertMetadataSql(DdmConstants.ATTRIBUTE_EXPOSE, consumer,
                DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE, getName());
    }

    private SqlStatement deleteMetadataForExposedSc(String consumer) {
        return DdmUtils.deleteMetadataSql(DdmConstants.ATTRIBUTE_EXPOSE, consumer, getName());
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

    public Boolean getTrembita() {
        return trembita;
    }

    public void setTrembita(Boolean trembita) {
        this.trembita = trembita;
    }

    public Boolean getPlatform() {
        return platform;
    }

    public void setPlatform(Boolean platform) {
        this.platform = platform;
    }

    public Boolean getExternalSystem() {
        return externalSystem;
    }

    public void setExternalSystem(Boolean externalSystem) {
        this.externalSystem = externalSystem;
    }

    public Boolean getPublicAccess() {
        return publicAccess;
    }

    public void setPublicAccess(Boolean publicAccess) {
        this.publicAccess = publicAccess;
    }
}