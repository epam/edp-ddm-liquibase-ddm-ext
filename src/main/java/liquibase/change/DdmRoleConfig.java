package liquibase.change;

import liquibase.DdmConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a new role.
 */
public class DdmRoleConfig extends AbstractLiquibaseSerializable {

    private String name;
    private List<DdmTableConfig> tables = new ArrayList<>();

    public DdmRoleConfig() {
        super();
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmRole";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_NAME)) {
                setName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
            } else if (child.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_TABLE)) {
                DdmTableConfig table = new DdmTableConfig();
                table.load(child, resourceAccessor);
                addTable(table);
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

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
