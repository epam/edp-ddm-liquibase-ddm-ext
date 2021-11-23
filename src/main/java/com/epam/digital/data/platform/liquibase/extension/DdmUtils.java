package com.epam.digital.data.platform.liquibase.extension;

import java.util.Objects;
import liquibase.changelog.ChangeSet;
import liquibase.statement.core.RawSqlStatement;
import liquibase.exception.ValidationErrors;

public class DdmUtils {

    private DdmUtils() {
    }

    public static RawSqlStatement insertMetadataSql(String changeType, String changeName, String attributeName, String attributeValue) {
        return new RawSqlStatement("insert into " + DdmConstants.METADATA_TABLE + "(" +
            DdmConstants.METADATA_CHANGE_TYPE + ", " +
            DdmConstants.METADATA_CHANGE_NAME + ", " +
            DdmConstants.METADATA_ATTRIBUTE_NAME + ", " +
            DdmConstants.METADATA_ATTRIBUTE_VALUE +
            ") values (" +
            "'" + changeType + "', " +
            "'" + changeName + "', " +
            "'" + attributeName + "', " +
            "'" + attributeValue + "');\n\n");
    }

    public static RawSqlStatement deleteMetadataByChangeTypeAndChangeNameSql(String changeType, String changeName) {
        return new RawSqlStatement("delete from " + DdmConstants.METADATA_TABLE + " where " +
            DdmConstants.METADATA_CHANGE_TYPE + " = '" + changeType + "' and " +
            DdmConstants.METADATA_CHANGE_NAME + " = '" + changeName + "';\n\n");
    }

    public static RawSqlStatement insertRolePermissionSql(String role, String table, String column, String operation) {
        return new RawSqlStatement("insert into " + DdmConstants.ROLE_PERMISSION_TABLE + "(" +
            DdmConstants.ROLE_PERMISSION_ROLE_NAME + ", " +
            DdmConstants.ROLE_PERMISSION_OBJECT_NAME + ", " +
            DdmConstants.ROLE_PERMISSION_COLUMN_NAME + ", " +
            DdmConstants.ROLE_PERMISSION_OPERATION +
            ") values (" +
            "'" + role + "', " +
            "'" + table + "', " +
            (Objects.isNull(column) ? "null" : "'" + column + "'") + ", " +
            "'" + operation + "');\n\n");
    }

    public static boolean hasContext(ChangeSet changeSet, String context) {
        return Boolean.TRUE.equals(changeSet.getChangeLog().getChangeLogParameters().getContexts().getContexts().contains(context));
    }

    public static ValidationErrors validateHistoryFlag(Boolean historyFlag) {
        return !Boolean.TRUE.equals(historyFlag) ?
            new ValidationErrors().addError("historyFlag attribute is required and must be set as 'true'") : new ValidationErrors();
    }
}
