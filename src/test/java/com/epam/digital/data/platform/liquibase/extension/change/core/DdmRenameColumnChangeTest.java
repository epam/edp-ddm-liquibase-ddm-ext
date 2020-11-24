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

package com.epam.digital.data.platform.liquibase.extension.change.core;

import liquibase.database.core.MockDatabase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmRenameColumnChangeTest {
    private DdmRenameColumnChange change;

    @BeforeEach
    void setUp() {
        change = new DdmRenameColumnChange();
        change.setTableName("table");
        change.setOldColumnName("oldColumn");
        change.setNewColumnName("newColumn");
    }

    @Test
    @DisplayName("Validate change")
    void validate() {
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - historyFlag=false")
    void validateHistoryFlagFalse() {
        change.setHistoryFlag(false);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }

    @Test
    @DisplayName("Validate change - historyFlag=true")
    void validateHistoryFlagTrue() {
        change.setHistoryFlag(true);
        Assertions.assertEquals(1, change.validate(new MockDatabase()).getErrorMessages().size());
    }
}