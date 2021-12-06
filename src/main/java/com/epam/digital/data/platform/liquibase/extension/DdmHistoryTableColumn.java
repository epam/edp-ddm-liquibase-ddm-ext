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

package com.epam.digital.data.platform.liquibase.extension;

public class DdmHistoryTableColumn {
    private String name;
    private String type;
    private String scope;
    private Boolean uniqueWithPrimaryKey;
    private Boolean nullable;
    private String defaultValueComputed;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public Boolean getUniqueWithPrimaryKey() {
        return uniqueWithPrimaryKey;
    }

    public void setUniqueWithPrimaryKey(Boolean uniqueWithPrimaryKey) {
        this.uniqueWithPrimaryKey = uniqueWithPrimaryKey;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public String getDefaultValueComputed() {
        return defaultValueComputed;
    }

    public void setDefaultValueComputed(String defaultValueComputed) {
        this.defaultValueComputed = defaultValueComputed;
    }
}
