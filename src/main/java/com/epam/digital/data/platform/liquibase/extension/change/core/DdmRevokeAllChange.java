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

import static com.epam.digital.data.platform.liquibase.extension.DdmUtils.hasSubContext;
import static com.epam.digital.data.platform.liquibase.extension.DdmUtils.isBlank;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmRoleConfig;
import java.util.ArrayList;
import java.util.List;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

/**
 * Creates a new revokeAll.
 */
@DatabaseChange(name="revokeAll", description = "revoke permissions to all report views", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmRevokeAllChange extends AbstractChange {

    private List<DdmRoleConfig> roles;

    public DdmRevokeAllChange() {
        super();
        this.roles = new ArrayList<>();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        getRoles().stream()
            .filter(role -> isBlank(role.getName()))
            .map(role -> "Role name cannot be empty")
            .forEach(validationErrors::addError);

        if (!DdmUtils.isAnalyticsChangeSet(this.getChangeSet())){
            validationErrors.addError(DdmUtils.printConsistencyChangeSetError(getChangeSet().getId()));
        }
        
        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        if (!hasSubContext(getChangeSet())) {
            this.getChangeSet().setIgnore(true);
            return new SqlStatement[0];
        }
        return getRoles().stream()
            .map(role -> new RawSqlStatement("CALL p_revoke_analytics_user ('" + role.getName() + "');"))
            .toArray(SqlStatement[]::new);
    }

    @Override
    public String getConfirmationMessage() {
        return "Permissions to all report views have been unset";
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_ROLE)) {
                DdmRoleConfig role = new DdmRoleConfig();
                role.load(child, resourceAccessor);
                addRole(role);
            }
        }
    }

    public void addRole(DdmRoleConfig role) {
        this.roles.add(role);
    }

    public List<DdmRoleConfig> getRoles() {
        return roles;
    }

    public void setRoles(List<DdmRoleConfig> roles) {
        this.roles = roles;
    }
}