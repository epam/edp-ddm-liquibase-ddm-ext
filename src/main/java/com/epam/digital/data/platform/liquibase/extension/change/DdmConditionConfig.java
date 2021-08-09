package com.epam.digital.data.platform.liquibase.extension.change;

import java.util.Objects;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmParameters;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;

import java.util.ArrayList;
import java.util.List;
import liquibase.serializer.AbstractLiquibaseSerializable;

public class DdmConditionConfig extends AbstractLiquibaseSerializable {
    private String logicOperator;
    private String tableAlias;
    private String columnName;
    private String operator;
    private String value;
    private List<DdmConditionConfig> conditions;

    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        setTableAlias(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_TABLE_ALIAS, String.class));
        setColumnName(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_COLUMN_NAME, String.class));
        setOperator(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_OPERATOR, String.class));
        setValue(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_VALUE, String.class));
        setLogicOperator(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_LOGIC_OPERATOR, String.class));
    }

    public static List<DdmConditionConfig> loadConditions(ParsedNode whereNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        if (whereNode == null) {
            return null;
        }

        List<DdmConditionConfig> conditionConfig = new ArrayList<>();
        for (ParsedNode child : whereNode.getChildren()) {
            if (child.getName().equals(DdmConstants.ATTRIBUTE_CONDITION)) {
                DdmConditionConfig condition = new DdmConditionConfig();
                condition.load(child, resourceAccessor);

                if (child.getChildren(null, DdmConstants.ATTRIBUTE_CONDITION).size() != 0) {
                    condition.setConditions(loadConditions(child, resourceAccessor));
                }

                conditionConfig.add(condition);
            }
        }

        return conditionConfig;
    }

    public boolean hasTableAlias() {
        return Objects.nonNull(getTableAlias()) && !DdmParameters.isEmpty(getTableAlias());
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getLogicOperator() {
        return logicOperator;
    }

    public void setLogicOperator(String logicOperator) {
        this.logicOperator = logicOperator;
    }

    public List<DdmConditionConfig> getConditions() {
        return conditions;
    }

    public void setConditions(List<DdmConditionConfig> conditions) {
        this.conditions = conditions;
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmCondition";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }
}
