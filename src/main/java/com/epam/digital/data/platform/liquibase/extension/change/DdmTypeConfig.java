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

package com.epam.digital.data.platform.liquibase.extension.change;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import liquibase.change.DatabaseChangeProperty;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.AbstractLiquibaseSerializable;
import liquibase.util.ObjectUtil;

import java.util.ArrayList;
import java.util.List;

public class DdmTypeConfig extends AbstractLiquibaseSerializable {
    private List<DdmColumnConfig> columns;
    private List<DdmLabelConfig> labels;

    public DdmTypeConfig() {
        labels = new ArrayList<>();
        columns = new ArrayList<>();
    }

    @Override
    public String getSerializedObjectName() {
        return "ddmType";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        for (ParsedNode child : parsedNode.getChildren()) {
            if (child.getName().equals(DdmConstants.ATTRIBUTE_LABEL)) {
                DdmLabelConfig label = new DdmLabelConfig();
                label.setLabel(child.getChildValue(null, DdmConstants.ATTRIBUTE_LABEL, String.class));

                if (label.getLabel() == null) {
                    label.setLabel((String) child.getValue());
                }

                label.setTranslation(child.getChildValue(null, DdmConstants.ATTRIBUTE_TRANSLATION, String.class));

                if (label.getTranslation() == null) {
                    label.setTranslation(label.getLabel());
                }

                addLabel(label);
            } else if (child.getName().equals(DdmConstants.ATTRIBUTE_COLUMN)) {
                DdmColumnConfig column = new DdmColumnConfig();
                column.setName(child.getChildValue(null, DdmConstants.ATTRIBUTE_NAME, String.class));
                column.setType(child.getChildValue(null, DdmConstants.ATTRIBUTE_TYPE, String.class));
                column.setCollation(child.getChildValue(null, DdmConstants.ATTRIBUTE_COLLATION, String.class));
                addColumn(column);
            } else if (!ObjectUtil.hasProperty(this, child.getName())) {
                throw new ParsedNodeException("Unexpected node: "+child.getName());
            }
        }
    }

    public void addColumn(DdmColumnConfig column) {
        columns.add(column);
    }

    @DatabaseChangeProperty(requiredForDatabase = "all")
    public List<DdmColumnConfig> getColumns() {
        return this.columns;
    }

    public void setColumns(List<DdmColumnConfig> columns) {
        this.columns = columns;
    }

    public void addLabel(DdmLabelConfig label) {
        labels.add(label);
    }

    @DatabaseChangeProperty(requiredForDatabase = "all")
    public List<DdmLabelConfig> getLabels() {
        return this.labels;
    }

    public void setLabels(List<DdmLabelConfig> labels) {
        this.labels = labels;
    }
}
