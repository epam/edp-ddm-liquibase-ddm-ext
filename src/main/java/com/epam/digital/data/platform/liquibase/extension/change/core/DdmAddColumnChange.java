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

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmParameters;
import com.epam.digital.data.platform.liquibase.extension.DdmRanChangeSetsKeeper;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmAddColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSequenceStatement;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import liquibase.Scope;
import liquibase.change.AddColumnConfig;
import liquibase.change.ChangeFactory;
import liquibase.change.ChangeMetaData;
import liquibase.change.ChangeParameterMetaData;
import liquibase.change.ChangeWithColumns;
import liquibase.change.ColumnConfig;
import liquibase.change.ConstraintsConfig;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddColumnChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.LiquibaseDataType;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.SetupException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.LiquibaseSerializable;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.SqlStatement;
import liquibase.statement.UniqueConstraint;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DropUniqueConstraintStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.RenameTableStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Index;
import liquibase.structure.core.Table;
import liquibase.util.JdbcUtils;

/**
 * Adds a column to an existing table with history.
 * 
 * 
 * 
 *     The version of the regulation is stored in the database.
 *     It is stored only after the successful deployment of the regulations.
 *     
 *     During the deployment of the regulations: 
 *
 *     If version == null it means the ChangeLog is running for the first time. 
 *     This means that the _hst table is empty and when run DdmAddColumnChange _hst table does not 
 *     need to be copied into the archive schema, instead of this we need to make an alter _hst 
 *     table simultaneously with alter origin table. The only difference is that the columns in the 
 *     historical table should not contain any constraints other than NOT NULL
 *
 *  
 *     If version != null it means some ChangeLog has already run before. 
 *     This means that when run DdmAddColumnChange, we must move the _hst table into the archive 
 *     schema and recreate the _hst table with the new column.
 *
 *     If the ChangeLog contains multiple DdmAddColumnChange (in different ChangeSets or in the same
 *     ChangeSet), then the first change moves the _hst table into the archive 
 *     schema and recreate the _hst table with the new column, and all other changes perform an
 *     alter _hst table simultaneously with alter origin table
 *
 *     If the ChangeLog contains more than one DdmAddColumnChange (in different ChangeSets) and the
 *     first ChangeSet has already run before, then the first DdmAddColumnChange is skipped and the 
 *     second one is executed as if it were the first one (the _hst table is recreated)
 */
@DatabaseChange(name="addColumn", description = "Adds a new column to an existing table with history", priority = ChangeMetaData.PRIORITY_DEFAULT + 50, appliesTo = "table")
public class DdmAddColumnChange extends AddColumnChange implements DdmArchiveAffectableChange {

    private static final String GRANT_USAGE_ON_SEQUENCE = "GRANT USAGE ON SEQUENCE %s_%s_seq TO %s;";
    private static final String ARCHIVE_SCHEMA = "archive";
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
        validationErrors.addAll(DdmUtils.validateHistoryFlag(getHistoryFlag()));
        validationErrors.addAll(validateConstraints());
        List<DdmAddColumnConfig> autoGenerated = getColumnsWithAutoGeneratedValues();
        validationErrors.addAll(validateColumnTypeForAutoGeneratedValues(autoGenerated));
        validationErrors.addAll(validateDateTimePatternsForAutoGeneratedValues(autoGenerated));

        String version = getVersion(database);
        if (version != null) {
            Boolean tableAlreadyPresentInArchiveSchema =
                isTablePresentInArchiveSchema(database, version);
            if (tableAlreadyPresentInArchiveSchema == null) {
                validationErrors.addError("Cannot select table!");
            }
            if (Boolean.TRUE.equals(tableAlreadyPresentInArchiveSchema)) {
                validationErrors.addError(
                    "ChangeLog with current version : "+version+" was already ran");
            }
        }
        return validationErrors;
    }
    
    private ValidationErrors validateConstraints() {
        ValidationErrors validationErrors = new ValidationErrors();
        for (AddColumnConfig column : getColumns()) {
            if (column.getConstraints() != null) {
                if (column.getDefaultValueObject() == null && column.getConstraints().isNullable() != null && Boolean.FALSE.equals(column.getConstraints().isNullable())) {
                    validationErrors.addError("Please set default value to not nullable column " + column.getName());
                }
                if (column.getDefaultValueObject() != null && column.getConstraints().isUnique() != null && Boolean.TRUE.equals(column.getConstraints().isUnique())) {
                    validationErrors.addError("Please choose one - either a default value or a unique one for the column " + column.getName());
                }
            }
        }
        return validationErrors;
    }

    private ValidationErrors validateColumnTypeForAutoGeneratedValues(List<DdmAddColumnConfig> generatedColumns) {
        ValidationErrors validationErrors = new ValidationErrors();
        for (DdmAddColumnConfig column : generatedColumns) {
            if (!column.getType().equalsIgnoreCase(DdmConstants.TYPE_TEXT)) {
                validationErrors.addError("Column '" + column.getName() + "' in table '" + getTableName()
                        + "' must be of type TEXT because it stores auto-generated values, but is of type '"
                        + column.getType() + "'");
            }
        }
        return validationErrors;
    }
    
    private ValidationErrors validateDateTimePatternsForAutoGeneratedValues(List<DdmAddColumnConfig> generatedColumns) {
        ValidationErrors validationErrors = new ValidationErrors();
        for (DdmAddColumnConfig column : generatedColumns) {
            String pattern = column.getAutoGenerate();
            pattern = pattern.replace("{SEQ}", "SEQ");
            while (pattern.contains("{") && pattern.contains("}")
                && pattern.indexOf("{") < pattern.indexOf("}")) {

                int beginIndex = pattern.indexOf("{");
                int endIndex = pattern.indexOf("}");
                String dateTimePattern = pattern.substring(beginIndex + 1, endIndex);
                try {
                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimePattern);
                    dateTimeFormatter.format(LocalDateTime.now());
                } catch (IllegalArgumentException e) {
                    validationErrors.addError(
                        String.format("Column '%s' contains a pattern '%s' that is not a date/time pattern", column.getName(), dateTimePattern));
                } catch (Exception ex) {
                    validationErrors.addError(
                        String.format("Column '%s' contains a pattern '%s' that cannot be applied to date/time formatting", column.getName(), dateTimePattern));
                }
                pattern = pattern.replace("{" + dateTimePattern + "}", "dateTime");
            }
        }
        return validationErrors;
    }
    
    private List<DdmAddColumnConfig> getColumnsWithAutoGeneratedValues() {
        return getColumns().stream()
            .filter(column -> DdmAddColumnConfig.class.isAssignableFrom(column.getClass()))
            .map(DdmAddColumnConfig.class::cast)
            .filter(c -> c.getAutoGenerate() != null)
            .collect(Collectors.toList());
    }

    private String getVersion(Database database) {
        String version = null;

        Statement statement = null;
        ResultSet resultSet = null;

        if (database.getConnection() instanceof JdbcConnection) {
            try {
                statement = ((JdbcConnection) database.getConnection()).createStatement(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

                String sql = "SELECT count(*) FROM pg_catalog.pg_tables where schemaname='" +
                    ARCHIVE_SCHEMA + "' and tablename like '" + getTableName() + "__%';";

                resultSet = statement.executeQuery(sql);

                if (resultSet.next()) {
                    version = "__" + resultSet.getInt("count");
                }
            } catch (SQLException | DatabaseException e) {
                Scope.getCurrentScope().getLog(database.getClass())
                    .info("Cannot select version", e);
            } finally {
                JdbcUtils.close(resultSet, statement);
            }
        }

        return version;
    }

    private Boolean isTablePresentInArchiveSchema(Database database, String version) {
        Boolean present = null;
        try {
            isHistoryTable = true;
            present = snapshotGeneratorFactory.createSnapshot(new Table(getCatalogName(), ARCHIVE_SCHEMA, getTableName() + version), database) != null;
        } catch (LiquibaseException e) {
            e.printStackTrace();
        } finally {
            isHistoryTable = false;
        }
        return present;
    }
    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>(Arrays.asList(super.generateStatements(database)));

        String version = getVersion(database);
        if (Boolean.TRUE.equals(historyFlag)) {
            boolean firstChange = DdmRanChangeSetsKeeper.isChangeFirst(database, this);
            
            isHistoryTable = true;
            
            if (!firstChange) {
                AddColumnChange addHstColumn = recreateAddColumnChangeWithNotNullConstraint(this);
                statements.addAll(Arrays.asList(addHstColumn.generateStatements(database)));
                isHistoryTable = false;
                statements.addAll(statementsForColumnsWithAutoGeneratedValues(getTableName(), getColumns()));
                return statements.toArray(new SqlStatement[0]);
            }
            
            String newTableName = getTableName() + version;

            Table snapshotTable = null;
            try {
                snapshotTable = snapshotGeneratorFactory.createSnapshot(new Table(getCatalogName(), getSchemaName(), getTableName()), database);
            } catch (Exception e) {
                Scope.getCurrentScope().getLog(database.getClass()).info("Cannot generate snapshot of table " + getTableName() + " on " + database.getShortName() + " database", e);
            }

            List<Index> uniqueConstraints = new ArrayList<>();
            if (snapshotTable != null) {
                uniqueConstraints = getHstTableUniqueConstraints(snapshotTable);
                uniqueConstraints.forEach(index -> statements.add(new DropUniqueConstraintStatement(null, null, getTableName(), index.getName())));
            }

            statements.add(new RenameTableStatement(null, null, getTableName(), newTableName));

            if (snapshotTable != null) {
                CreateTableStatement createHstTableStatement = new CreateTableStatement(null, null, snapshotTable.getName(), snapshotTable.getRemarks());

                for (Column column : snapshotTable.getColumns()) {
                    LiquibaseDataType columnType = DataTypeFactory.getInstance().fromDescription(column.getType().toString(), database);
                    createHstTableStatement.addColumn(column.getName(), columnType, column.getDefaultValueConstraintName(), column.getDefaultValue(), column.getRemarks());

                    if (!column.isNullable()) {
                        NotNullConstraint notNullConstraint = new NotNullConstraint(column.getName());
                        notNullConstraint.setValidateNullable(column.getValidateNullable());
                        createHstTableStatement.addColumnConstraint(notNullConstraint);
                    }
                }

                for (ColumnConfig column : getColumns()) {
                    LiquibaseDataType columnType = DataTypeFactory.getInstance().fromDescription(column.getType(), database);
                    createHstTableStatement.addColumn(column.getName(), columnType, column.getDefaultValueConstraintName(), column.getDefaultValue(), column.getRemarks());

                    ConstraintsConfig constraints = column.getConstraints();
                    if (constraints != null && constraints.isNullable() != null && !constraints.isNullable()) {
                        NotNullConstraint notNullConstraint = new NotNullConstraint(column.getName());
                        notNullConstraint.setConstraintName(constraints.getNotNullConstraintName());
                        notNullConstraint.setValidateNullable(constraints.getValidateNullable() == null || constraints.getValidateNullable());
                        createHstTableStatement.addColumnConstraint(notNullConstraint);
                    }
                }
                recreateHstUniqueConstraints(uniqueConstraints)
                        .forEach(createHstTableStatement::addColumnConstraint);

                statements.add(createHstTableStatement);
                statements.addAll(createTableAccessStatements(snapshotTable.getName()));
                statements.add(new RawSqlStatement("CALL p_init_new_hist_table('" + newTableName + "', '" + snapshotTable.getName() + "');"));
                statements.add(new RawSqlStatement("ALTER TABLE " + newTableName + " SET SCHEMA " + ARCHIVE_SCHEMA + ";"));
            }
            isHistoryTable = false;
        }

        statements.addAll(statementsForColumnsWithAutoGeneratedValues(getTableName(), getColumns()));
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
    
    private AddColumnChange recreateAddColumnChangeWithNotNullConstraint(AddColumnChange origin) {
        AddColumnChange result = new AddColumnChange();
        result.setTableName(origin.getTableName());
        result.setSchemaName(origin.getSchemaName());
        result.setCatalogName(origin.getCatalogName());
        result.setChangeSet(origin.getChangeSet());
        
        result.setColumns(
            origin.getColumns().stream()
                .map(this::retainNotNullConstraint)
                .collect(Collectors.toList()));
        return result;
    }

    AddColumnConfig retainNotNullConstraint(AddColumnConfig column) {
        AddColumnConfig clone = new AddColumnConfig(new Column(column));
        
        ConstraintsConfig originConstraints = column.getConstraints();
        if (originConstraints != null && Boolean.FALSE.equals(originConstraints.isNullable())) {
            ConstraintsConfig resultConstraints = 
                new ConstraintsConfig()
                    .setNullable(false)
                    .setNotNullConstraintName(originConstraints.getNotNullConstraintName())
                    .setValidateNullable(originConstraints.getValidateNullable() == null || originConstraints.getValidateNullable());
            clone.setConstraints(resultConstraints);
        }
        
        return clone;
    }

    @Override
    public void load(
        ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        ChangeMetaData metaData = Scope.getCurrentScope().getSingleton(ChangeFactory.class).getChangeMetaData(this);

        try {
            Collection<ChangeParameterMetaData> changeParameters = metaData.getParameters().values();

            for (ChangeParameterMetaData param : changeParameters) {
                if (Collection.class.isAssignableFrom(param.getDataTypeClass())) {
                    if (param.getDataTypeClassParameters().length == 1) {
                        Class collectionType = (Class) param.getDataTypeClassParameters()[0];
                        if (ColumnConfig.class.isAssignableFrom(collectionType)) {
                            List<ParsedNode> columnNodes = new ArrayList<>(
                                parsedNode.getChildren(null, param.getParameterName())
                            );
                            columnNodes.addAll(parsedNode.getChildren(null, NODENAME_COLUMN));

                            Object nodeValue = parsedNode.getValue();
                            if (nodeValue instanceof ParsedNode) {
                                columnNodes.add((ParsedNode) nodeValue);
                            } else if (nodeValue instanceof Collection) {
                                for (Object nodeValueChild : ((Collection) nodeValue)) {
                                    if (nodeValueChild instanceof ParsedNode) {
                                        columnNodes.add((ParsedNode) nodeValueChild);
                                    }
                                }
                            }

                            for (ParsedNode child : columnNodes) {
                                if (NODENAME_COLUMN.equals(child.getName()) || "columns".equals(child.getName())) {
                                    List<ParsedNode> columnChildren = child.getChildren(null, NODENAME_COLUMN);
                                    if ((columnChildren != null) && !columnChildren.isEmpty()) {
                                        for (ParsedNode columnChild : columnChildren) {
                                            DdmAddColumnConfig columnConfig = new DdmAddColumnConfig();
                                            columnConfig.load(columnChild, resourceAccessor);
                                            ((ChangeWithColumns) this).addColumn(columnConfig);
                                        }
                                    } else {
                                        DdmAddColumnConfig columnConfig = new DdmAddColumnConfig();
                                        columnConfig.load(child, resourceAccessor);
                                        ((ChangeWithColumns) this).addColumn(columnConfig);
                                    }
                                }
                            }
                        } else if (
                            (LiquibaseSerializable.class.isAssignableFrom(collectionType))
                                && (!collectionType.isInterface())
                                && (!Modifier.isAbstract(collectionType.getModifiers()))
                        ) {
                            String elementName = ((LiquibaseSerializable) collectionType.getConstructor().newInstance())
                                .getSerializedObjectName();
                            List<ParsedNode> nodes = new ArrayList<>(
                                parsedNode.getChildren(null, param.getParameterName())
                            );
                            if (!elementName.equals(param.getParameterName())) {
                                nodes.addAll(parsedNode.getChildren(null, elementName));
                            }

                            Object nodeValue = parsedNode.getValue();
                            if (nodeValue instanceof ParsedNode) {
                                nodes.add((ParsedNode) nodeValue);
                            } else if (nodeValue instanceof Collection) {
                                for (Object nodeValueChild : ((Collection) nodeValue)) {
                                    if (nodeValueChild instanceof ParsedNode) {
                                        nodes.add((ParsedNode) nodeValueChild);
                                    }
                                }
                            }

                            for (ParsedNode node : nodes) {
                                if (node.getName().equals(elementName)
                                    || node.getName().equals(param.getParameterName())) {
                                    List<ParsedNode> childNodes = node.getChildren(null, elementName);
                                    if ((childNodes != null) && !childNodes.isEmpty()) {
                                        for (ParsedNode childNode : childNodes) {
                                            LiquibaseSerializable childObject =
                                                (LiquibaseSerializable)collectionType.getConstructor().newInstance();
                                            childObject.load(childNode, resourceAccessor);
                                            ((Collection) param.getCurrentValue(this)).add(childObject);
                                        }
                                    } else {
                                        LiquibaseSerializable childObject =
                                            (LiquibaseSerializable) collectionType.getConstructor().newInstance();
                                        childObject.load(node, resourceAccessor);
                                        ((Collection) param.getCurrentValue(this)).add(childObject);
                                    }
                                }
                            }
                        }
                    }
                } else if (LiquibaseSerializable.class.isAssignableFrom(param.getDataTypeClass())) {
                    if (!param.getDataTypeClass().isInterface()
                        && !Modifier.isAbstract(param.getDataTypeClass().getModifiers())) {

                        try {
                            ParsedNode child = parsedNode.getChild(null, param.getParameterName());
                            if (child != null) {
                                LiquibaseSerializable serializableChild =
                                    (LiquibaseSerializable) param.getDataTypeClass().getConstructor().newInstance();
                                serializableChild.load(child, resourceAccessor);
                                param.setValue(this, serializableChild);
                            }
                        } catch (ReflectiveOperationException e) {
                            throw new UnexpectedLiquibaseException(e);
                        }
                    }
                } else {
                    Object childValue = parsedNode.getChildValue(
                        null, param.getParameterName(), param.getDataTypeClass()
                    );
                    if ((childValue == null) && (param.getSerializationType() == SerializationType.DIRECT_VALUE)) {
                        childValue = parsedNode.getValue();
                    }
                    if(null != childValue) {
                        param.setValue(this, childValue);
                    }
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new UnexpectedLiquibaseException(e);
        }
        customLoadLogic(parsedNode, resourceAccessor);
        try {
            this.finishInitialization();
        } catch (SetupException e) {
            throw new ParsedNodeException(e);
        }
    }

    private List<Index> getHstTableUniqueConstraints(Table snapshotTable) {
        return snapshotTable.getIndexes().stream()
                .filter(Index::isUnique)
                .collect(Collectors.toList());
    }

    private List<UniqueConstraint> recreateHstUniqueConstraints(List<Index> uniqueConstraintsIndex) {
        return uniqueConstraintsIndex.stream()
                .map(index -> {
                    String[] columnNames = index.getColumns()
                            .stream()
                            .map(Column::getName)
                            .toArray(String[]::new);
                    liquibase.statement.UniqueConstraint uc = new liquibase.statement.UniqueConstraint(index.getName());
                    uc.addColumns(columnNames);
                    return uc;
                })
                .collect(Collectors.toList());
    }

    private List<SqlStatement> createTableAccessStatements(String tableName) {
        List<SqlStatement> accessStatements = new ArrayList<>();
        accessStatements.add(new RawSqlStatement("REVOKE ALL PRIVILEGES ON TABLE " + tableName + " FROM PUBLIC;"));

        if (DdmUtils.hasContext(this.getChangeSet(), DdmConstants.CONTEXT_PUB)) {
            accessStatements.add(new RawSqlStatement("GRANT SELECT ON " + tableName + " TO application_role;"));
        }

        if (DdmUtils.hasContext(this.getChangeSet(), DdmConstants.CONTEXT_SUB)) {
            accessStatements.add(new RawSqlStatement("GRANT SELECT ON " + tableName + " TO historical_data_role;"));
        }
        return accessStatements;
    }

    protected List<SqlStatement> statementsForColumnsWithAutoGeneratedValues(String tableName, List<AddColumnConfig> columns) {
        List<SqlStatement> result = new ArrayList<>();
        for (AddColumnConfig columnConfig : columns) {
            if (DdmAddColumnConfig.class.isAssignableFrom(columnConfig.getClass())) {
                DdmAddColumnConfig column = (DdmAddColumnConfig) columnConfig;
                if (column.getAutoGenerate() != null) {
                    result.add(new DdmCreateSequenceStatement(tableName, column.getName()));
                    result.add(new RawSqlStatement(
                        String.format(GRANT_USAGE_ON_SEQUENCE, tableName, column.getName(),
                            "application_role")));
                    result.add(DdmUtils.insertMetadataSql(DdmConstants.ATTRIBUTE_AUTOGENERATE,
                        getTableName(), column.getName(), column.getAutoGenerate()));
                }
            }
        }
        return result;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        DdmAddColumnChange that = (DdmAddColumnChange) o;
        return isHistoryTable == that.isHistoryTable && Objects.equals(historyFlag,
            that.historyFlag) && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), historyFlag, parameters, isHistoryTable);
    }
}