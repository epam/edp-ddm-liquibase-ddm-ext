package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import java.util.Objects;

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
import liquibase.statement.core.DeleteStatement;

import java.util.ArrayList;
import java.util.List;

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
                    if (Objects.nonNull(table.getRoleCanRead()) && Objects.nonNull(column.getRoleCanRead())) {
                        validationErrors.addError("Values for read are ambiguous: role=" + role.getName() +
                            ", table=" + table.getName() +
                            ", column=" + column.getName());
                    }

                    if (Objects.nonNull(table.getRoleCanUpdate()) && Objects.nonNull(column.getRoleCanUpdate())) {
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
        statements.add(new DeleteStatement(null, null, DdmConstants.ROLE_PERMISSION_TABLE));

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

        return statements.toArray(new SqlStatement[statements.size()]);
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