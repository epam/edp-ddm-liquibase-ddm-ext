/*
 * Copyright 2023 EPAM Systems.
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

package com.epam.digital.data.platform.liquibase.extension.change;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;

import java.util.ArrayList;
import java.util.List;

public class DdmLogicOperatorConfig extends AbstractLiquibaseSerializable {

    private String type;
    private List<DdmColumnConfig> columns;
    private List<DdmLogicOperatorConfig> logicOperators;

    public void load(ParsedNode parsedNode, ResourceAccessor accessor) throws ParsedNodeException {
        setType(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_TYPE, String.class));
        List<DdmColumnConfig> columns = new ArrayList<>();
        for (ParsedNode xmlColumn : parsedNode.getChildren(null, DdmConstants.ATTRIBUTE_COLUMN)) {
            DdmColumnConfig column = new DdmColumnConfig();
            column.load(xmlColumn, accessor);
            columns.add(column);
        }
        setColumns(columns);
    }

    public static List<DdmLogicOperatorConfig> loadLogicOperators(ParsedNode parsedNode,
                                                                  ResourceAccessor resourceAccessor) throws ParsedNodeException {
        List<DdmLogicOperatorConfig> logicOperators = new ArrayList<>();
        for (ParsedNode logicOperatorNode : parsedNode.getChildren(null, DdmConstants.ATTRIBUTE_LOGIC_OPERATOR)) {
            DdmLogicOperatorConfig logicOperator = new DdmLogicOperatorConfig();
            logicOperator.load(logicOperatorNode, resourceAccessor);
            logicOperator.setLogicOperators(loadLogicOperators(logicOperatorNode, resourceAccessor));
            logicOperators.add(logicOperator);
        }
        return logicOperators;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<DdmColumnConfig> getColumns() {
        return columns;
    }

    public void setColumns(List<DdmColumnConfig> columns) {
        this.columns = columns;
    }

    public List<DdmLogicOperatorConfig> getLogicOperators() {
        return logicOperators;
    }

    public void setLogicOperators(List<DdmLogicOperatorConfig> logicOperators) {
        this.logicOperators = logicOperators;
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmSearchConditionLogicOperator";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }
}
