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

import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmRoleConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.DdmPair;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;

import java.util.ArrayList;
import java.util.List;
import liquibase.statement.core.RawSqlStatement;

/**
 * Creates a new RBAC.
 */

@DatabaseChange(name="rbac", description = "rbac - set permissions", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmRbacChange extends AbstractChange {

    private List<DdmRoleConfig> roles;

    public DdmRbacChange() {
        super();
        this.roles = new ArrayList<>();
    }

    private ValidationErrors validateConflicts() {
        ValidationErrors validationErrors = new ValidationErrors();

        List<DdmPair> rolesTables = new ArrayList<>();
        for (DdmRoleConfig role : getRoles()) {
            for (DdmTableConfig table : role.getTables()) {
                DdmPair roleTable = new DdmPair(role.getName(), table.getName());

                if (rolesTables.contains(roleTable)) {
                    validationErrors.addError("There are doubled values: role=" + role.getName() +
                        ", table=" + table.getName());
                } else {
                    rolesTables.add(roleTable);
                }

                for (DdmColumnConfig column : table.getColumns()) {
                    if (table.getRoleCanRead() != null && column.getRoleCanRead() != null) {
                        validationErrors.addError("Values for read are ambiguous: role=" + role.getName() +
                            ", table=" + table.getName() +
                            ", column=" + column.getName());
                    }

                    if (table.getRoleCanUpdate() != null && column.getRoleCanUpdate() != null) {
                        validationErrors.addError("Values for update are ambiguous: role - " + role.getName() +
                            ", table - " + table.getName() +
                            ", column - " + column.getName());
                    }
                }
            }
        }
        return validationErrors;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));
        validationErrors.addAll(validateConflicts());
        return validationErrors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();
        statements.add(new RawSqlStatement("DELETE FROM " + DdmConstants.ROLE_PERMISSION_TABLE));

        for (DdmRoleConfig role : getRoles()) {
            for (DdmTableConfig table : role.getTables()) {
                if (Boolean.TRUE.equals(table.getRoleCanInsert())) {
                    statements.add(DdmUtils.insertRolePermissionSql(role.getName(), table.getName(), null, "I"));
                }

                if (Boolean.TRUE.equals(table.getRoleCanDelete())) {
                    statements.add(DdmUtils.insertRolePermissionSql(role.getName(), table.getName(), null, "D"));
                }

                if (Boolean.TRUE.equals(table.getRoleCanRead())) {
                    statements.add(DdmUtils.insertRolePermissionSql(role.getName(), table.getName(), null, "S"));
                }

                if (Boolean.TRUE.equals(table.getRoleCanUpdate())) {
                    statements.add(DdmUtils.insertRolePermissionSql(role.getName(), table.getName(), null, "U"));
                }

                for (DdmColumnConfig column : table.getColumns()) {
                    if (Boolean.TRUE.equals(column.getRoleCanRead())) {
                        statements.add(DdmUtils.insertRolePermissionSql(role.getName(), table.getName(), column.getName(), "S"));
                    }

                    if (Boolean.TRUE.equals(column.getRoleCanUpdate())) {
                        statements.add(DdmUtils.insertRolePermissionSql(role.getName(), table.getName(), column.getName(), "U"));
                    }
                }
            }
        }

        return statements.toArray(new SqlStatement[0]);
    }

    @Override
    public String getConfirmationMessage() {
        return "Permissions have been set";
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

    public List<DdmRoleConfig> getRoles() {
        return roles;
    }

    public void setRoles(List<DdmRoleConfig> roles) {
        this.roles = roles;
    }

    public void addRole(DdmRoleConfig role) {
        this.roles.add(role);
    }
}