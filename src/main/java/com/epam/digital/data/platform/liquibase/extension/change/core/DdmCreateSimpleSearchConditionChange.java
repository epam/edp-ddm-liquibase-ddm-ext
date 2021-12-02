package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSimpleSearchConditionStatement;
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

/**
 * Creates a new simple search condition.
 */
@DatabaseChange(name="createSimpleSearchCondition", description = "Create Simple Search Condition", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateSimpleSearchConditionChange extends AbstractChange {

    private String name;
    private DdmTableConfig table;
    private DdmColumnConfig searchColumn;
    private Boolean indexing;
    private String limit;

    public DdmCreateSimpleSearchConditionChange() {
        super();
    }

    public DdmCreateSimpleSearchConditionChange(String name) {
        super();
        this.name = name;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        if (Boolean.TRUE.equals(indexing) && getSearchColumn() == null) {
            validationErrors.addError("searchColumn is not defined!");
        }

        if (Boolean.TRUE.equals(indexing) && getSearchColumn() != null && getSearchColumn().getSearchType() == null) {
            validationErrors.addError("searchType is not defined!");
        }
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
        DdmCreateSimpleSearchConditionStatement statement = generateCreateSimpleSearchConditionStatement();
        statement.setTable(getTable());
        statement.setSearchColumn(getSearchColumn());
        statement.setIndexing(getIndexing());
        statement.setLimit(getLimit());

        statements.add(statement);

        //  create insert statement for metadata table
        if (statement.getSearchColumn() != null && statement.getSearchColumn().getSearchType() != null) {
            String val;
            if (getSearchColumn().getSearchType().equals(DdmConstants.ATTRIBUTE_EQUAL)) {
                val = DdmConstants.ATTRIBUTE_EQUAL_COLUMN;
            } else if (getSearchColumn().getSearchType().equals(DdmConstants.ATTRIBUTE_CONTAINS)) {
                val = DdmConstants.ATTRIBUTE_CONTAINS_COLUMN;
            } else {
                val = DdmConstants.ATTRIBUTE_STARTS_WITH_COLUMN;
            }

            statements.add(DdmUtils.insertMetadataSql(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE, getName(), val, getSearchColumn().getName()));
        }

        if (getLimit() != null && !getLimit().equalsIgnoreCase(DdmConstants.ATTRIBUTE_ALL)) {
            statements.add(DdmUtils.insertMetadataSql(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE, getName(), DdmConstants.SEARCH_METADATA_ATTRIBUTE_NAME_LIMIT, getLimit()));
        }

        return statements.toArray(new SqlStatement[0]);
    }

    protected DdmCreateSimpleSearchConditionStatement generateCreateSimpleSearchConditionStatement() {
        return new DdmCreateSimpleSearchConditionStatement(getName());
    }

    @Override
    public String getConfirmationMessage() {
        return "Simple Search Condition " + getName() + " created";
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_TABLE)) {
                DdmTableConfig table = new DdmTableConfig();
                table.setName(child.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
                table.setAlias(child.getChildValue(null, DdmConstants.ATTRIBUTE_ALIAS, String.class));
                setTable(table);

                DdmColumnConfig column = new DdmColumnConfig();
                column.setName(child.getChildValue(null, DdmConstants.ATTRIBUTE_SEARCH_COLUMN, String.class));
                column.setType(child.getChildValue(null, DdmConstants.ATTRIBUTE_TYPE, String.class));
                column.setSearchType(child.getChildValue(null, DdmConstants.ATTRIBUTE_SEARCH_TYPE, String.class));
                setSearchColumn(column);
            }
        }
    }

    public DdmTableConfig getTable() {
        return this.table;
    }

    public void setTable(DdmTableConfig table) {
        this.table = table;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getIndexing() {
        return indexing;
    }

    public void setIndexing(Boolean indexing) {
        this.indexing = indexing;
    }

    public DdmColumnConfig getSearchColumn() {
        return searchColumn;
    }

    public void setSearchColumn(DdmColumnConfig searchColumn) {
        this.searchColumn = searchColumn;
    }

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }
}
