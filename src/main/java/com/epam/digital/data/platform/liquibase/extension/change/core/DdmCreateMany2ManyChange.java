package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateMany2ManyStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new create many to many relation.
 */
@DatabaseChange(name="createMany2Many", description = "Create Many To Many Relation", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateMany2ManyChange extends AbstractChange {

    private String mainTableName;
    private String mainTableKeyField;
    private String referenceTableName;
    private String referenceKeysArray;
    private List<DdmColumnConfig> mainTableColumns = new ArrayList<>();
    private List<DdmColumnConfig> referenceTableColumns = new ArrayList<>();

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        DdmCreateMany2ManyStatement statement = new DdmCreateMany2ManyStatement();
        statement.setMainTableName(getMainTableName());
        statement.setMainTableKeyField(getMainTableKeyField());
        statement.setReferenceTableName(getReferenceTableName());
        statement.setReferenceKeysArray(getReferenceKeysArray());
        statement.setMainTableColumns(getMainTableColumns());
        statement.setReferenceTableColumns(getReferenceTableColumns());

        statements.add(statement);

        return statements.toArray(new SqlStatement[statements.size()]);
    }

    public String getRelationName() {
        return mainTableName + "_" + referenceTableName + DdmConstants.SUFFIX_RELATION;
    }

    @Override
    public String getConfirmationMessage() {
        return "Many To Many Relation " + getRelationName() + " created";
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        for (ParsedNode child : parsedNode.getChildren()) {
            DdmTableConfig tableConfig = new DdmTableConfig();
            tableConfig.load(child, resourceAccessor);

            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_MAIN_TABLE_COLUMNS)) {
                setMainTableColumns(tableConfig.getColumns());
            } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_REFERENCE_TABLE_COLUMNS)) {
                setReferenceTableColumns(tableConfig.getColumns());
            }
        }
    }

    public String getMainTableName() {
        return mainTableName;
    }

    public void setMainTableName(String mainTableName) {
        this.mainTableName = mainTableName;
    }

    public String getMainTableKeyField() {
        return mainTableKeyField;
    }

    public void setMainTableKeyField(String mainTableKeyField) {
        this.mainTableKeyField = mainTableKeyField;
    }

    public String getReferenceTableName() {
        return referenceTableName;
    }

    public void setReferenceTableName(String referenceTableName) {
        this.referenceTableName = referenceTableName;
    }

    public String getReferenceKeysArray() {
        return referenceKeysArray;
    }

    public void setReferenceKeysArray(String referenceKeysArray) {
        this.referenceKeysArray = referenceKeysArray;
    }

    public List<DdmColumnConfig> getMainTableColumns() {
        return mainTableColumns;
    }

    public void setMainTableColumns(List<DdmColumnConfig> mainTableColumns) {
        this.mainTableColumns = mainTableColumns;
    }

    public List<DdmColumnConfig> getReferenceTableColumns() {
        return referenceTableColumns;
    }

    public void setReferenceTableColumns(List<DdmColumnConfig> referenceTableColumns) {
        this.referenceTableColumns = referenceTableColumns;
    }
}
