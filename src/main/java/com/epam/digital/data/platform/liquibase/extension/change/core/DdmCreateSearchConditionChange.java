package com.epam.digital.data.platform.liquibase.extension.change.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmConditionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmCteConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmFunctionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import liquibase.change.AbstractChange;
import liquibase.change.Change;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSearchConditionStatement;

import java.util.ArrayList;
import java.util.List;
import liquibase.statement.core.RawSqlStatement;
import liquibase.util.StringUtil;

/**
 * Creates a new search condition.
 */
@DatabaseChange(name="createSearchCondition", description = "Create Search Condition", priority = ChangeMetaData.PRIORITY_DEFAULT)
public class DdmCreateSearchConditionChange extends AbstractChange {

    private List<DdmCteConfig> ctes;
    private List<DdmTableConfig> tables;
    private List<DdmJoinConfig> joins;
    private String name;
    private Boolean indexing;
    private String limit;
    private Boolean pagination;
    private List<DdmConditionConfig> conditions;

    public DdmCreateSearchConditionChange() {
        super();
        this.ctes = new ArrayList<>();
        this.tables = new ArrayList<>();
        this.joins = new ArrayList<>();
    }

    public DdmCreateSearchConditionChange(String name) {
        super();
        this.name = name;
        this.ctes = new ArrayList<>();
        this.tables = new ArrayList<>();
        this.joins = new ArrayList<>();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addAll(super.validate(database));

        getJoins().stream().filter(join -> join.getLeftColumns().size() != join.getRightColumns().size())
            .map(join -> "Different amount of columns in join clause").forEach(validationErrors::addError);

        if (Boolean.TRUE.equals(getIndexing())) {
            boolean hasSearchColumns = false;

            for (DdmTableConfig table : getTables()) {
                if (table.getColumns().stream().anyMatch(column -> column.getSearchType() != null)) {
                    hasSearchColumns = true;
                }
            }

            if (!hasSearchColumns) {
                validationErrors.addError("no search column is defined!");
            }
        }

        for (DdmTableConfig table : getTables()) {
            for (DdmFunctionConfig function : table.getFunctions()) {
                if (function.getName().equals(DdmConstants.ATTRIBUTE_FUNCTION_STRING_AGG) &&
                    StringUtil.isEmpty(function.getParameter())) {
                    validationErrors.addError("function " + function.getName().toUpperCase() + " required additional parameter!");
                } else if (function.hasParameter()) {
                    validationErrors.addError("function " + function.getName().toUpperCase() + " doesn't required additional parameter!");
                }
            }
        }
        return validationErrors;
    }

    private RawSqlStatement insertSearchConditionMetadata(String attributeName, String attributeValue) {
        return DdmUtils.insertMetadataSql(DdmConstants.SEARCH_METADATA_CHANGE_TYPE_VALUE, getName(), attributeName, attributeValue);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<SqlStatement> statements = new ArrayList<>();

        DdmCreateSearchConditionStatement statement = generateCreateSearchConditionStatement();
        statement.setCtes(getCtes());
        statement.setTables(getTables());
        statement.setJoins(getJoins());
        statement.setIndexing(getIndexing());
        statement.setLimit(getLimit());
        statement.setConditions(getConditions());

        statements.add(statement);
        statements.add(new RawSqlStatement("GRANT SELECT ON " + statement.getViewName() + " TO application_role;"));

        if (DdmUtils.hasContext(this.getChangeSet(), DdmConstants.CONTEXT_SUB)) {
            statements.add(new RawSqlStatement("GRANT SELECT ON " + statement.getViewName() + " TO analytics_admin;"));
        }

        //  create insert statements for metadata table
        for (DdmTableConfig table : getTables()) {
            for (DdmColumnConfig column : table.getColumns()) {
                statements.add(insertSearchConditionMetadata(DdmConstants.ATTRIBUTE_COLUMN, column.getNameOrAlias()));

                if (Boolean.TRUE.equals(column.getReturning())) {
                    statements.add(DdmUtils.insertMetadataSql(getName(), table.getName(), column.getName(), column.getNameOrAlias()));
                }

                if (column.getSearchType() != null) {
                    String val;
                    if (DdmConstants.ATTRIBUTE_EQUAL.equals(column.getSearchType())) {
                        val = DdmConstants.ATTRIBUTE_EQUAL_COLUMN;
                    } else if (DdmConstants.ATTRIBUTE_CONTAINS.equals(column.getSearchType())) {
                        val = DdmConstants.ATTRIBUTE_CONTAINS_COLUMN;
                    } else {
                        val = DdmConstants.ATTRIBUTE_STARTS_WITH_COLUMN;
                    }

                    statements.add(insertSearchConditionMetadata(val, column.getNameOrAlias()));
                }
            }
        }

        if (getLimit() != null && !getLimit().equalsIgnoreCase(DdmConstants.ATTRIBUTE_ALL)) {
            statements.add(insertSearchConditionMetadata(DdmConstants.SEARCH_METADATA_ATTRIBUTE_NAME_LIMIT, getLimit()));
        }

        if (Boolean.TRUE.equals(pagination)) {
            statements.add(insertSearchConditionMetadata(DdmConstants.SEARCH_METADATA_ATTRIBUTE_NAME_PAGINATION, Boolean.toString(true)));
        }

        return statements.toArray(new SqlStatement[0]);
    }

    protected DdmCreateSearchConditionStatement generateCreateSearchConditionStatement() {
        return new DdmCreateSearchConditionStatement(getName());
    }

    @Override
    protected Change[] createInverses() {
        DdmDropSearchConditionChange inverse = new DdmDropSearchConditionChange();
        inverse.setName(getName());
        return new Change[]{ inverse };
    }

    @Override
    public String getConfirmationMessage() {
        return "Search Condition " + getName() + " created";
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_CTE)) {
                DdmCteConfig cte = new DdmCteConfig();
                cte.load(child, resourceAccessor);
                addCte(cte);
            } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_TABLE)) {
                DdmTableConfig table = new DdmTableConfig();
                table.load(child, resourceAccessor);
                addTable(table);
            } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_JOIN)) {
                DdmJoinConfig join = new DdmJoinConfig();
                join.load(child, resourceAccessor);
                addJoin(join);
            } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_WHERE)) {
                setConditions(DdmConditionConfig.loadConditions(child, resourceAccessor));
            }
        }
    }

    public List<DdmCteConfig> getCtes() {
        return ctes;
    }

    public void setCtes(List<DdmCteConfig> ctes) {
        this.ctes = ctes;
    }

    public void addCte(DdmCteConfig cte) {
        this.ctes.add(cte);
    }

    public List<DdmTableConfig> getTables() {
        return this.tables;
    }

    public void setTables(List<DdmTableConfig> tables) {
        this.tables = tables;
    }

    public void addTable(DdmTableConfig table) {
        this.tables.add(table);
    }

    public List<DdmJoinConfig> getJoins() {
        return this.joins;
    }

    public void setJoins(List<DdmJoinConfig> joins) {
        this.joins = joins;
    }

    public void addJoin(DdmJoinConfig join) {
        this.joins.add(join);
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

    public String getLimit() {
        return limit;
    }

    public void setLimit(String limit) {
        this.limit = limit;
    }

    public List<DdmConditionConfig> getConditions() {
        return conditions;
    }

    public void setConditions(List<DdmConditionConfig> conditions) {
        this.conditions = conditions;
    }

    public Boolean getPagination() {
        return pagination;
    }

    public void setPagination(Boolean pagination) {
        this.pagination = pagination;
    }
}
