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
import java.util.Collection;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmParameters;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.DdmHistoryTableColumn;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmDistributeTableStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmReferenceTableStatement;
import liquibase.Scope;
import liquibase.change.AddColumnConfig;
import liquibase.change.Change;
import liquibase.change.ChangeFactory;
import liquibase.change.ChangeMetaData;
import liquibase.change.ChangeParameterMetaData;
import liquibase.change.ChangeWithColumns;
import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.change.DatabaseChangeProperty;
import liquibase.change.core.CreateTableChange;
import liquibase.change.core.DropTableChange;
import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.SetupException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.ColumnConstraint;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.ForeignKeyConstraint;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.SqlStatement;
import liquibase.statement.UniqueConstraint;
import liquibase.statement.core.AddDefaultValueStatement;
import liquibase.statement.core.CreateIndexStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DropPrimaryKeyStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.SetColumnRemarksStatement;
import liquibase.statement.core.SetTableRemarksStatement;
import liquibase.util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Creates a new table with history.
 */
@DatabaseChange(name="createTable", description = "Create Table With History",
    priority = ChangeMetaData.PRIORITY_DEFAULT+50)
public class DdmCreateTableChange extends CreateTableChange {

    private Boolean historyFlag;
    private Boolean isObject;
    private final ThreadLocal<Boolean> historyTable = new ThreadLocal<>();
    private final DdmParameters parameters = new DdmParameters();
    private String distribution;

    public DdmCreateTableChange() {
        super();
        historyTable.set(false);
    }

    @DatabaseChangeProperty(requiredForDatabase = "ddm")
    public Boolean getHistoryFlag() {
        return historyFlag;
    }

    public void setHistoryFlag(Boolean historyFlag) {
        this.historyFlag = historyFlag;
    }

    public String getDistribution() {
        return distribution;
    }

    public void setDistribution(String distribution) {
        this.distribution = distribution;
    }

    private ValidationErrors validatePrimaryKey(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();

        SqlStatement[] statements = super.generateStatements(database);
        CreateTableStatement statement = (CreateTableStatement) statements[0];

        if (statement.getPrimaryKeyConstraint() == null) {
            validationErrors.addError("Table " + getTableName() + " does not have primary key");
        }
        return validationErrors;
    }

    private boolean fieldExists(CreateTableStatement statement, String fieldName) {
        return statement.getColumns().stream().anyMatch(column -> column.equalsIgnoreCase(fieldName));
    }

    private boolean hasClassify() {
        return getColumns().stream().map(column -> (DdmColumnConfig) column)
            .anyMatch(col -> col.getClassify() != null);
    }

    private void generateClassifyDefaultValues(List<SqlStatement> statements) {
        String defaultValue = getColumns().stream().map(column -> (DdmColumnConfig) column)
            .filter(col -> col.getClassify() != null)
            .map(col -> "\"(" + col.getName() + "," + col.getClassify() + ")\"").collect(Collectors.joining(", "));

        if (!StringUtil.isEmpty(defaultValue)) {
            defaultValue = "{" + defaultValue + "}";
            statements.add(new AddDefaultValueStatement(null, null, getTableName(), DdmConstants.FIELD_CLASSIFICATION, DdmConstants.TYPE_TEXT, defaultValue));
        }
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));
        validationErrors.addAll(DdmUtils.validateHistoryFlag(getHistoryFlag()));
        validationErrors.addAll(validatePrimaryKey(database));

        if (!Boolean.TRUE.equals(getHistoryFlag()) && getDistribution() != null
            && (getDistribution().equals(DdmConstants.DISTRIBUTION_DISTRIBUTE_ALL)
            || getDistribution().equals(DdmConstants.DISTRIBUTION_DISTRIBUTE_HISTORY)
            || getDistribution().equals(DdmConstants.DISTRIBUTION_REFERENCE_ALL)
            || getDistribution().equals(DdmConstants.DISTRIBUTION_REFERENCE_HISTORY))) {
            validationErrors.addError("distribution cannot be applied since history flag is not enabled");
        }
        return validationErrors;
    }

    protected CreateTableStatement createStatement(Database database) {
        SqlStatement[] statements = super.generateStatements(database);
        CreateTableStatement statement = (CreateTableStatement) statements[0];

        UniqueConstraint uc = new UniqueConstraint(DdmConstants.PREFIX_UNIQUE_INDEX + getTableName());
        if (statement.getPrimaryKeyConstraint() != null) {
            statement.getPrimaryKeyConstraint().getColumns().forEach(uc::addColumns);
        }

        for (DdmHistoryTableColumn column : parameters.getHistoryTableColumns()) {
            if (StringUtil.isEmpty(column.getScope())
                || DdmParameters.isAll(column.getScope())
                || (DdmParameters.isPrimary(column.getScope()) && (!historyTable.get()))
                || (DdmParameters.isHistory(column.getScope()) && (historyTable.get()))) {
                statement.addColumn(
                    column.getName(),
                    DataTypeFactory.getInstance().fromDescription(column.getType(), database),
                    !StringUtil.isEmpty(column.getDefaultValueComputed()) ? new DatabaseFunction(column.getDefaultValueComputed()) : null);
            }

            if (!column.getNullable()) {
                statement.addColumnConstraint(new NotNullConstraint(column.getName()));
            }

            if (column.getUniqueWithPrimaryKey()) {
                uc.addColumns(column.getName());
            }
        }

        if (hasClassify()) {
            parameters.getDcmColumns().forEach(column -> statement.addColumn(
                column.getName(),
                DataTypeFactory.getInstance().fromDescription(column.getType(), database),
                null));

            setIsObject(true);
        }

        if (Boolean.TRUE.equals(getIsObject())) {
            if (!fieldExists(statement, parameters.getSubjectColumn())) {
                statement.addColumn(
                    parameters.getSubjectColumn(),
                    DataTypeFactory.getInstance().fromDescription(parameters.getSubjectColumnType(), database),
                    new ColumnConstraint[]{new NotNullConstraint(parameters.getSubjectColumn())});
            }

            if (!historyTable.get()) {
                ForeignKeyConstraint fkConstraint = new ForeignKeyConstraint(
                    "fk_" + getTableName() + "_" + parameters.getSubjectTable(),
                    null,
                    parameters.getSubjectTable(),
                    parameters.getSubjectColumn());
                fkConstraint.setColumn(parameters.getSubjectColumn());

                statement.addColumnConstraint(fkConstraint);
            }
        }

        if (historyTable.get()) {
            statement.getUniqueConstraints().clear();
            statement.addColumnConstraint(uc);
        }

        return statement;
    }

    private void generateRemarks(List<SqlStatement> statements, Database database) {
        if (StringUtil.trimToNull(getRemarks()) != null) {
            SetTableRemarksStatement remarksStatement = new SetTableRemarksStatement(getCatalogName(), getSchemaName(), getTableName(), getRemarks());
            if (SqlGeneratorFactory.getInstance().supports(remarksStatement, database)) {
                statements.add(remarksStatement);
            }
        }

        for (ColumnConfig column : getColumns()) {
            String columnRemarks = StringUtil.trimToNull(column.getRemarks());
            if (columnRemarks != null) {
                SetColumnRemarksStatement remarksStatement = new SetColumnRemarksStatement(getCatalogName(), getSchemaName(), getTableName(), column.getName(), columnRemarks);
                if (!(database instanceof MySQLDatabase) && SqlGeneratorFactory.getInstance().supports(remarksStatement, database)) {
                    statements.add(remarksStatement);
                }
            }
        }
    }

    private void generateAccess(List<SqlStatement> statements, String tableName) {
        statements.add(new RawSqlStatement("REVOKE ALL PRIVILEGES ON TABLE " + tableName + " FROM PUBLIC;"));

        if (DdmUtils.hasPubContext(this.getChangeSet())) {
            statements.add(new RawSqlStatement("GRANT SELECT ON " + tableName + " TO application_role;"));
        }

        if (historyTable.get() && DdmUtils.hasSubContext(this.getChangeSet())) {
            statements.add(new RawSqlStatement("GRANT SELECT ON " + tableName + " TO historical_data_role;"));
        }
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        if (Boolean.TRUE.equals(getHistoryFlag())) {
            List<SqlStatement> statements = new ArrayList<>();

            historyTable.set(true);

            String pkName = null;
            CreateTableStatement statement;

            statement = createStatement(database);
            statement.getForeignKeyConstraints().clear();
            statements.add(statement);

            if (statement.getPrimaryKeyConstraint() != null) {
                pkName = StringUtil.trimToNull(statement.getPrimaryKeyConstraint().getConstraintName());
            }

            if (pkName == null) {
                pkName = database.generatePrimaryKeyName(getTableName());
            }

            statements.add(new DropPrimaryKeyStatement(getCatalogName(), getSchemaName(), getTableName(), pkName));

            if (getDistribution() != null) {
                if (getDistribution().equals(DdmConstants.DISTRIBUTION_DISTRIBUTE_ALL) || getDistribution().equals(DdmConstants.DISTRIBUTION_DISTRIBUTE_HISTORY)) {
                    statements.add(new DdmDistributeTableStatement(getTableName(), statement.getPrimaryKeyConstraint().getColumns().get(0)));
                } else if (getDistribution().equals(DdmConstants.DISTRIBUTION_REFERENCE_ALL) || getDistribution().equals(DdmConstants.DISTRIBUTION_REFERENCE_HISTORY)) {
                    statements.add(new DdmReferenceTableStatement(getTableName()));
                }
            }

            generateRemarks(statements, database);
            generateAccess(statements, getTableName());

            historyTable.set(false);

            statement = createStatement(database);
            statements.add(statement);

            for (ForeignKeyConstraint foreignKey : statement.getForeignKeyConstraints()) {
                AddColumnConfig column = new AddColumnConfig();
                column.setName(foreignKey.getColumn());

                statements.add(new CreateIndexStatement(
                    DdmConstants.PREFIX_INDEX + getTableName() + "_" + foreignKey.getReferencedTableName() + "__" + foreignKey.getColumn(),
                    null,
                    null,
                    getTableName(),
                    false,
                    null,
                    column)
                );
            }

            generateClassifyDefaultValues(statements);

            if (getDistribution() != null) {
                if (getDistribution().equals(DdmConstants.DISTRIBUTION_DISTRIBUTE_ALL) || getDistribution().equals(DdmConstants.DISTRIBUTION_DISTRIBUTE_PRIMARY)) {
                    statements.add(new DdmDistributeTableStatement(getTableName(), statement.getPrimaryKeyConstraint().getColumns().get(0)));
                } else if (getDistribution().equals(DdmConstants.DISTRIBUTION_REFERENCE_ALL) || getDistribution().equals(DdmConstants.DISTRIBUTION_REFERENCE_PRIMARY)) {
                    statements.add(new DdmReferenceTableStatement(getTableName()));
                }
            }

            generateRemarks(statements, database);
            generateAccess(statements, getTableName());

            return statements.toArray(new SqlStatement[0]);
        } else {
            return super.generateStatements(database);
        }
    }

    protected DropTableChange createDropChange(String tableName) {
        DropTableChange change = new DropTableChange();
        change.setCatalogName(getCatalogName());
        change.setSchemaName(getSchemaName());
        change.setTableName(tableName);

        return change;
    }

    @Override
    protected Change[] createInverses() {
        if (Boolean.TRUE.equals(getHistoryFlag())) {
            historyTable.set(false);
            DropTableChange inverse = createDropChange(getTableName());
            historyTable.set(true);
            DropTableChange inverseHistory = createDropChange(getTableName());
            historyTable.set(false);

            return new Change[]{ inverse, inverseHistory };
        } else {
            return super.createInverses();
        }
    }

    @Override
    public String getTableName() {
        return super.getTableName().toLowerCase() + (Boolean.TRUE.equals(getHistoryFlag()) && historyTable.get() ? parameters.getHistoryTableSuffix() : "");
    }

    public Boolean getIsObject() {
        return isObject;
    }

    public void setIsObject(Boolean isObject) {
        this.isObject = isObject;
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

                            for (ParsedNode child : columnNodes) {
                                if (NODENAME_COLUMN.equals(child.getName()) || "columns".equals(child.getName())) {
                                    List<ParsedNode> columnChildren = child.getChildren(null, NODENAME_COLUMN);
                                    if ((columnChildren != null) && !columnChildren.isEmpty()) {
                                        for (ParsedNode columnChild : columnChildren) {
                                            DdmColumnConfig columnConfig = new DdmColumnConfig();
                                            columnConfig.load(columnChild, resourceAccessor);
                                            ((ChangeWithColumns) this).addColumn(columnConfig);
                                        }
                                    } else {
                                        DdmColumnConfig columnConfig = new DdmColumnConfig();
                                        columnConfig.load(child, resourceAccessor);
                                        ((ChangeWithColumns) this).addColumn(columnConfig);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    Object childValue = parsedNode.getChildValue(
                        null, param.getParameterName(), param.getDataTypeClass()
                    );
                    if ((childValue == null) && (param.getSerializationType() == SerializationType.DIRECT_VALUE)) {
                        childValue = parsedNode.getValue();
                    }
                    if(childValue != null) {
                        param.setValue(this, childValue);
                    }
                }
            }
        } catch (Exception e) {
            throw new UnexpectedLiquibaseException(e);
        }
        customLoadLogic(parsedNode, resourceAccessor);
        try {
            this.finishInitialization();
        } catch (SetupException e) {
            throw new ParsedNodeException(e);
        }
    }
}
