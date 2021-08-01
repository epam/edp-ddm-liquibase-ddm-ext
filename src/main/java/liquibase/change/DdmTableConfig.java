package liquibase.change;

import static liquibase.DdmParameters.isEmpty;

import java.util.Objects;
import liquibase.DdmConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;

import java.util.ArrayList;
import java.util.List;
import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmTableConfig extends AbstractLiquibaseSerializable {

    private String schemaName;
    private String name;
    private String alias;
    private List<DdmColumnConfig> columns;
    private List<DdmFunctionConfig> functions;
    private Boolean usedInSQLClause;
    private Boolean roleCanInsert;
    private Boolean roleCanDelete;
    private Boolean roleCanRead;
    private Boolean roleCanUpdate;

    public DdmTableConfig() {
        this.columns = new ArrayList<>();
        this.functions = new ArrayList<>();
        this.usedInSQLClause = false;
    }

    public DdmTableConfig(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
        this.functions = new ArrayList<>();
        this.usedInSQLClause = false;
    }

    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
        setAlias(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_ALIAS, String.class));
        setRoleCanInsert(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_INSERT, Boolean.class));
        setRoleCanDelete(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_DELETE, Boolean.class));
        setRoleCanRead(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_READ, Boolean.class));
        setRoleCanUpdate(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_UPDATE, Boolean.class));

        for (ParsedNode xmlColumn : parsedNode.getChildren(null, DdmConstants.ATTRIBUTE_COLUMN)) {
            DdmColumnConfig column = new DdmColumnConfig();
            column.load(xmlColumn, resourceAccessor);
            addColumn(column);
        }

        for (ParsedNode xmlColumn : parsedNode.getChildren(null, DdmConstants.ATTRIBUTE_FUNCTION)) {
            DdmFunctionConfig function = new DdmFunctionConfig();
            function.load(xmlColumn, resourceAccessor);
            function.setTableAlias(getAlias());
            addFunction(function);
        }
    }

    public boolean hasAlias() {
        return Objects.nonNull(getAlias()) && !isEmpty(getAlias());
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<DdmColumnConfig> getColumns() {
        return columns;
    }

    public void setColumns(List<DdmColumnConfig> columns) {
        this.columns = columns;
    }

    public void addColumn(DdmColumnConfig column) {
        this.columns.add(column);
    }

    public List<DdmFunctionConfig> getFunctions() {
        return functions;
    }

    public void setFunctions(List<DdmFunctionConfig> functions) {
        this.functions = functions;
    }

    public void addFunction(DdmFunctionConfig function) {
        this.functions.add(function);
    }

    public Boolean getUsedInSQLClause() {
        return usedInSQLClause;
    }

    public void setUsedInSQLClause(Boolean usedInSQLClause) {
        this.usedInSQLClause = usedInSQLClause;
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmTable";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    public Boolean getRoleCanInsert() {
        return roleCanInsert;
    }

    public void setRoleCanInsert(Boolean roleCanInsert) {
        this.roleCanInsert = roleCanInsert;
    }

    public Boolean getRoleCanDelete() {
        return roleCanDelete;
    }

    public void setRoleCanDelete(Boolean roleCanDelete) {
        this.roleCanDelete = roleCanDelete;
    }

    public Boolean getRoleCanRead() {
        return roleCanRead;
    }

    public void setRoleCanRead(Boolean roleCanRead) {
        this.roleCanRead = roleCanRead;
    }

    public Boolean getRoleCanUpdate() {
        return roleCanUpdate;
    }

    public void setRoleCanUpdate(Boolean roleCanUpdate) {
        this.roleCanUpdate = roleCanUpdate;
    }
}
