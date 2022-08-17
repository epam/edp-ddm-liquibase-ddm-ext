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
import liquibase.change.AddColumnConfig;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;

public class DdmAddColumnConfig extends AddColumnConfig {

    private String autoGenerate;
    
    public DdmAddColumnConfig() {
        super();
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);
        setAutoGenerate(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_AUTOGENERATE, String.class));
    }

    public String getAutoGenerate() {
        return autoGenerate;
    }

    public void setAutoGenerate(String autoGenerate) {
        this.autoGenerate = autoGenerate;
    }
}
