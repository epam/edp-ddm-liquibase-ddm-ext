package liquibase.change;

import liquibase.DdmConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new cte.
 */
public class DdmCteConfig extends AbstractLiquibaseSerializable {

    private List<DdmTableConfig> tables;
    private List<DdmJoinConfig> joins;
    private String name;
    private String limit;
    private List<DdmConditionConfig> conditions;

    public DdmCteConfig() {
        super();
        this.tables = new ArrayList<>();
        this.joins = new ArrayList<>();
        this.conditions = new ArrayList<>();
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmCte";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_NAME)) {
                setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
            } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_LIMIT)) {
                setLimit(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_LIMIT, String.class));
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
}
