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

import java.util.ArrayList;
import java.util.List;

public class DdmLogicOperatorSerializableConfig {

    private String type;
    private List<String> columns = new ArrayList<>();
    private List<DdmLogicOperatorSerializableConfig> logicOperators = new ArrayList<>();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<DdmLogicOperatorSerializableConfig> getLogicOperators() {
        return logicOperators;
    }

    public void setLogicOperators(
            List<DdmLogicOperatorSerializableConfig> logicOperators) {
        this.logicOperators = logicOperators;
    }
}
