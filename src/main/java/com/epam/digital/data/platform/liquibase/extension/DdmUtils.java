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

package com.epam.digital.data.platform.liquibase.extension;

import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmAddColumnChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmCreateMany2ManyChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmCreateTableChange;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmCreateSearchConditionChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmCreateSimpleSearchConditionChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmDropSearchConditionChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmExposeSearchConditionChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmGrantAllChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmGrantChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmRevokeAllChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmRevokeChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmCreateAnalyticsIndexChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmCreateAnalyticsViewChange;
import com.epam.digital.data.platform.liquibase.extension.change.core.DdmDropAnalyticsViewChange;

import java.util.stream.Collectors;
import liquibase.change.AbstractChange;
import liquibase.change.ColumnConfig;
import liquibase.change.core.AddColumnChange;
import liquibase.changelog.ChangeSet;
import liquibase.statement.core.RawSqlStatement;
import liquibase.exception.ValidationErrors;

public class DdmUtils {

    private DdmUtils() {
    }

    private static final Set<Class<? extends AbstractChange>> masterChanges = new HashSet<>();
    private static final Set<Class<? extends AbstractChange>> replicaChanges = new HashSet<>();
    public static final String CONSISTENCY_CHANGESET_ERROR = "Error. ChangeSet %s. Analytics and Search Condition changes "
        + "cannot be mixed with each other or other change types in a single changeset. "
        + "Please put them in separate changesets.";

    private static final Map<String, String> mapSearchTypeToMetadata = new HashMap<>();

    private static final Set<String> textCastableTypes = new HashSet<>();

    static {
        masterChanges.add(DdmCreateSearchConditionChange.class);
        masterChanges.add(DdmCreateSimpleSearchConditionChange.class);
        masterChanges.add(DdmDropSearchConditionChange.class);
        masterChanges.add(DdmExposeSearchConditionChange.class);

        replicaChanges.add(DdmCreateAnalyticsViewChange.class);
        replicaChanges.add(DdmCreateAnalyticsIndexChange.class);
        replicaChanges.add(DdmDropAnalyticsViewChange.class);
        replicaChanges.add(DdmGrantChange.class);
        replicaChanges.add(DdmGrantAllChange.class);
        replicaChanges.add(DdmRevokeChange.class);
        replicaChanges.add(DdmRevokeAllChange.class);

        mapSearchTypeToMetadata.put(DdmConstants.ATTRIBUTE_EQUAL, DdmConstants.ATTRIBUTE_EQUAL_COLUMN);
        mapSearchTypeToMetadata.put(DdmConstants.ATTRIBUTE_CONTAINS, DdmConstants.ATTRIBUTE_CONTAINS_COLUMN);
        mapSearchTypeToMetadata.put(DdmConstants.ATTRIBUTE_STARTS_WITH, DdmConstants.ATTRIBUTE_STARTS_WITH_COLUMN);
        mapSearchTypeToMetadata.put(DdmConstants.ATTRIBUTE_STARTS_WITH_ARRAY, DdmConstants.ATTRIBUTE_STARTS_WITH_ARRAY_COLUMN);
        mapSearchTypeToMetadata.put(DdmConstants.ATTRIBUTE_IN, DdmConstants.ATTRIBUTE_IN_COLUMN);
        mapSearchTypeToMetadata.put(DdmConstants.ATTRIBUTE_BETWEEN, DdmConstants.ATTRIBUTE_BETWEEN_COLUMN);
        mapSearchTypeToMetadata.put(DdmConstants.ATTRIBUTE_NOT_IN, DdmConstants.ATTRIBUTE_NOT_IN_COLUMN);
        mapSearchTypeToMetadata.put(DdmConstants.ATTRIBUTE_NOT_EQUAL, DdmConstants.ATTRIBUTE_NOT_EQUAL_COLUMN);

        textCastableTypes.add(DdmConstants.ATTRIBUTE_EQUAL);
        textCastableTypes.add(DdmConstants.ATTRIBUTE_IN);
        textCastableTypes.add(DdmConstants.ATTRIBUTE_BETWEEN);
        textCastableTypes.add(DdmConstants.ATTRIBUTE_NOT_IN);
        textCastableTypes.add(DdmConstants.ATTRIBUTE_NOT_EQUAL);
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

    public static RawSqlStatement deleteMetadataSql(String changeType, String changeName) {
        StringBuilder builder = new StringBuilder();
        builder.append("delete from ").append(DdmConstants.METADATA_TABLE);
        builder.append(" where (").append(DdmConstants.METADATA_CHANGE_TYPE).append(" = '")
                .append(changeType).append("') and (");
        builder.append(DdmConstants.METADATA_CHANGE_NAME).append(" = '").append(changeName).append("');\n\n");

        return new RawSqlStatement(builder.toString());
    }

    public static RawSqlStatement deleteMetadataSql(String changeType, String changeName, String attributeValue) {
        StringBuilder builder = new StringBuilder();
        builder.append("delete from ").append(DdmConstants.METADATA_TABLE);
        builder.append(" where (").append(DdmConstants.METADATA_CHANGE_TYPE).append(" = '")
                .append(changeType).append("') and (");
        builder.append(DdmConstants.METADATA_CHANGE_NAME).append(" = '").append(changeName).append("') and (");
        builder.append(DdmConstants.METADATA_ATTRIBUTE_VALUE).append(" = '").append(attributeValue).append("');\n\n");

        return new RawSqlStatement(builder.toString());
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

    public static boolean hasPubContext(ChangeSet changeSet){
        return hasContext(changeSet, DdmConstants.CONTEXT_PUB);
    }

    public static boolean hasSubContext(ChangeSet changeSet){
        return hasContext(changeSet, DdmConstants.CONTEXT_SUB);
    }

    public static ValidationErrors validateHistoryFlag(Boolean historyFlag) {
        return !Boolean.TRUE.equals(historyFlag) ?
            new ValidationErrors().addError("historyFlag attribute is required and must be set as 'true'") : new ValidationErrors();
    }

    public static boolean isAnalyticsChangeSet(ChangeSet changeSet) {
        return changeSet.getChanges().stream()
            .allMatch(change -> replicaChanges.contains(change.getClass()));
    }

    public static boolean isSearchConditionChangeSet(ChangeSet changeSet){
        return changeSet.getChanges().stream()
            .allMatch(change -> masterChanges.contains(change.getClass()));
    }

    public static String printConsistencyChangeSetError(String changeSetId){
        return String.format(CONSISTENCY_CHANGESET_ERROR, changeSetId);
    }

    public static boolean isBlank(String string) {
        return string == null || string.trim().isEmpty();
    }

    public static List<DdmCreateTableChange> getCreateTableChangesFromChangeLog(ChangeSet changeSet, List<String> tableNames) {
        return changeSet.getChangeLog().getRootChangeLog().getChangeSets().stream()
            .flatMap(set -> set.getChanges().stream()).flatMap(change -> tableNames.stream()
                .filter(tableName -> change instanceof DdmCreateTableChange &&
                    ((DdmCreateTableChange) change).getTableName().equals(tableName))
                .map(tableName -> (DdmCreateTableChange) change).collect(Collectors.toList()).stream())
            .collect(Collectors.toList());
    }

    public static List<DdmAddColumnChange> getAddColumnChangesFromChangeLog(ChangeSet changeSet, List<String> tableNames) {
        return changeSet.getChangeLog().getRootChangeLog().getChangeSets().stream()
                .flatMap(set -> set.getChanges().stream()).flatMap(change -> tableNames.stream()
                        .filter(tableName -> change instanceof DdmAddColumnChange &&
                                ((DdmAddColumnChange) change).getTableName().equals(tableName))
                        .map(tableName -> (DdmAddColumnChange) change).collect(Collectors.toList()).stream())
                .collect(Collectors.toList());
    }

    public static DdmCreateMany2ManyChange getM2mChangeFromChangelogForNestedRead(
            ChangeSet changeSet, String tableName, String columnName) {
        return changeSet.getChangeLog().getRootChangeLog().getChangeSets().stream()
                .flatMap(set -> set.getChanges().stream())
                .filter(DdmCreateMany2ManyChange.class::isInstance)
                .map(DdmCreateMany2ManyChange.class::cast)
                .filter(change -> change.getMainTableName().equals(tableName)
                        && change.getReferenceKeysArray().equals(columnName))
                .findFirst()
                .orElse(null);
    }

    public static ValidationErrors validationForNestedReadColumn(ChangeSet changeSet, String tableName, ColumnConfig column) {
        boolean columnNotContainsM2m = DdmUtils.getM2mChangeFromChangelogForNestedRead(
                changeSet, tableName, column.getName()) == null;
        boolean columnIsNotForeignKey = column.getConstraints() == null
                || column.getConstraints().getForeignKeyName() == null;
        if (columnNotContainsM2m && columnIsNotForeignKey) {
            return new ValidationErrors().addError(
                    String.format("Column %s.%s should have many to many " +
                                    "or one to many relation to perform entity fetch from column",
                            tableName, column.getName()));
        } else {
            return new ValidationErrors();
        }
    }

    public static List<AddColumnChange> getColumnChangesFromChangeLog(ChangeSet changeSet, List<String> tableNames) {
        return changeSet.getChangeLog().getRootChangeLog().getChangeSets().stream()
            .flatMap(set -> set.getChanges().stream()).flatMap(change -> tableNames.stream()
                .filter(tableName -> change instanceof AddColumnChange &&
                    ((AddColumnChange) change).getTableName().equals(tableName))
                .map(tableName -> (AddColumnChange) change).collect(Collectors.toList()).stream())
            .collect(Collectors.toList());
    }

    public static String mapLiquibaseSearchTypeToMetadataType(DdmColumnConfig column) {
        return mapSearchTypeToMetadata.get(column.getSearchType());
    }

    public static boolean isColumnAvailableForCasting(DdmColumnConfig column) {
        return textCastableTypes.contains(column.getSearchType()) &&
                Arrays.stream(DdmTypesForIndexCast.values())
                        .anyMatch(type -> type.getValue().equalsIgnoreCase(column.getType()));
    }
}
