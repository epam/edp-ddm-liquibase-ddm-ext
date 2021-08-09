package com.epam.digital.data.platform.liquibase.extension.change;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;

import java.util.ArrayList;
import java.util.List;
import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmJoinConfig extends AbstractLiquibaseSerializable {

    private String type;
    private String leftAlias;
    private List<String> leftColumns;
    private String rightAlias;
    private List<String> rightColumns;
    private List<DdmConditionConfig> conditions;

    public DdmJoinConfig() {
        this.leftColumns = new ArrayList<>();
        this.rightColumns = new ArrayList<>();
    }

    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        this.setType(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_TYPE, String.class));

        for (ParsedNode joinLeft : parsedNode.getChildren(null, DdmConstants.ATTRIBUTE_LEFT)) {
            this.setLeftAlias(joinLeft.getChildValue(null, DdmConstants.ATTRIBUTE_ALIAS, String.class));
            for (ParsedNode column : joinLeft.getChildren(null, DdmConstants.ATTRIBUTE_COLUMN)) {
                this.addLeftColumn(column.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
            }
        }

        for (ParsedNode joinRight : parsedNode.getChildren(null, DdmConstants.ATTRIBUTE_RIGHT)) {
            this.setRightAlias(joinRight.getChildValue(null, DdmConstants.ATTRIBUTE_ALIAS, String.class));
            for (ParsedNode column : joinRight.getChildren(null, DdmConstants.ATTRIBUTE_COLUMN)) {
                this.addRightColumn(column.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
            }
        }

        this.setConditions(DdmConditionConfig.loadConditions(parsedNode, resourceAccessor));
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLeftAlias() {
        return leftAlias;
    }

    public void setLeftAlias(String leftAlias) {
        this.leftAlias = leftAlias;
    }

    public List<String> getLeftColumns() {
        return leftColumns;
    }

    public void setLeftColumns(List<String> leftColumns) {
        this.leftColumns = leftColumns;
    }

    public void addLeftColumn(String column) {
        this.leftColumns.add(column);
    }

    public String getRightAlias() {
        return rightAlias;
    }

    public void setRightAlias(String rightAlias) {
        this.rightAlias = rightAlias;
    }

    public List<String> getRightColumns() {
        return rightColumns;
    }

    public void setRightColumns(List<String> rightColumns) {
        this.rightColumns = rightColumns;
    }

    public void addRightColumn(String column) {
        this.rightColumns.add(column);
    }

    public List<DdmConditionConfig> getConditions() {
        return conditions;
    }

    public void setConditions(List<DdmConditionConfig> conditions) {
        this.conditions = conditions;
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmJoin";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }
}
