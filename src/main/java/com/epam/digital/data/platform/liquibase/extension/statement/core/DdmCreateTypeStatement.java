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

package com.epam.digital.data.platform.liquibase.extension.statement.core;

import com.epam.digital.data.platform.liquibase.extension.change.DdmTypeConfig;
import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.CompoundStatement;

public class DdmCreateTypeStatement extends AbstractSqlStatement implements CompoundStatement {
    private final String name;
    private final DdmTypeConfig asComposite;
    private final DdmTypeConfig asEnum;

    public DdmCreateTypeStatement(String name, DdmTypeConfig asComposite, DdmTypeConfig asEnum) {
        this.name = name;
        this.asComposite = asComposite;
        this.asEnum = asEnum;
    }

    public String getName() {
        return name;
    }

    public DdmTypeConfig getAsComposite() {
        return asComposite;
    }

    public DdmTypeConfig getAsEnum() {
        return asEnum;
    }
}
