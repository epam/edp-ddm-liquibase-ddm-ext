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

package com.epam.digital.data.platform.liquibase.extension.change.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmAlterTableAttrConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DdmAlterTableChangeTest {

  private DdmAlterTableChange alterTableChange;
  private ChangeSet changeSet;

  @BeforeEach
  void setUp() {
    DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
    changeSet = new ChangeSet(changeLog);
    alterTableChange = new DdmAlterTableChange();
    changeSet.addChange(alterTableChange);
    DdmCreateTableChange tableChange = new DdmCreateTableChange();
    tableChange.setTableName("test");
    changeSet.addChange(tableChange);
    changeLog.addChangeSet(changeSet);
  }

  @Test
  @DisplayName("Validate - attribute bulkLoad has an invalid value")
  void validateBulkLoadAttribute() {
    DdmAlterTableAttrConfig readModeAttribute = new DdmAlterTableAttrConfig();
    readModeAttribute.setName("readMode");
    readModeAttribute.setValue("async");
    DdmAlterTableAttrConfig bulkLoadAttribute = new DdmAlterTableAttrConfig();
    bulkLoadAttribute.setName("bulkLoad");
    bulkLoadAttribute.setValue("fail");
    String table = "test";
    alterTableChange.setTable(table);
    alterTableChange.setAttributeList(Arrays.asList(readModeAttribute, bulkLoadAttribute));

    List<String> errorMessages = alterTableChange.validate(new MockDatabase()).getErrorMessages();

    assertEquals(1, errorMessages.size());
    assertEquals("Attribute [bulkLoad] has an invalid value [fail]", errorMessages.get(0));
  }

  @Test
  @DisplayName("Validate - attribute readMode has an invalid value")
  void validateReadModeAttribute() {
    DdmAlterTableAttrConfig readModeAttribute = new DdmAlterTableAttrConfig();
    readModeAttribute.setName("readMode");
    readModeAttribute.setValue("fail");
    DdmAlterTableAttrConfig bulkLoadAttribute = new DdmAlterTableAttrConfig();
    bulkLoadAttribute.setName("bulkLoad");
    bulkLoadAttribute.setValue("true");
    String table = "test";
    alterTableChange.setTable(table);
    alterTableChange.setAttributeList(Arrays.asList(readModeAttribute, bulkLoadAttribute));

    List<String> errorMessages = alterTableChange.validate(new MockDatabase()).getErrorMessages();

    assertEquals(1, errorMessages.size());
    assertEquals("Attribute [readMode] has an invalid value [fail]", errorMessages.get(0));
  }

  @Test
  @DisplayName("Validate - table does not exist")
  void validateTableNameAttribute() {
    DdmAlterTableAttrConfig readModeAttribute = new DdmAlterTableAttrConfig();
    readModeAttribute.setName("readMode");
    readModeAttribute.setValue("sync");
    DdmAlterTableAttrConfig bulkLoadAttribute = new DdmAlterTableAttrConfig();
    bulkLoadAttribute.setName("bulkLoad");
    bulkLoadAttribute.setValue("true");
    String table = "fail";
    alterTableChange.setTable(table);
    alterTableChange.setAttributeList(Arrays.asList(readModeAttribute, bulkLoadAttribute));

    List<String> errorMessages = alterTableChange.validate(new MockDatabase()).getErrorMessages();

    assertEquals(1, errorMessages.size());
    assertEquals("Table [fail] does not exist", errorMessages.get(0));
  }

  @Test
  @DisplayName("Validate - all attributes are valid")
  void validateAttributesValid() {
    DdmAlterTableAttrConfig readModeAttribute = new DdmAlterTableAttrConfig();
    readModeAttribute.setName("readMode");
    readModeAttribute.setValue("read");
    DdmAlterTableAttrConfig bulkLoadAttribute = new DdmAlterTableAttrConfig();
    bulkLoadAttribute.setName("bulkLoad");
    bulkLoadAttribute.setValue("bulk");
    String table = "test";
    alterTableChange.setTable(table);
    alterTableChange.setAttributeList(Arrays.asList(readModeAttribute, bulkLoadAttribute));

    List<String> errorMessages = alterTableChange.validate(new MockDatabase()).getErrorMessages();

    assertEquals(2, errorMessages.size());
    assertEquals("Attribute [readMode] has an invalid value [read]", errorMessages.get(0));
    assertEquals("Attribute [bulkLoad] has an invalid value [bulk]", errorMessages.get(1));
  }

  @Test
  @DisplayName("Validate - all attributes are invalid")
  void validateAttributesInvalid() {
    DdmAlterTableAttrConfig readModeAttribute = new DdmAlterTableAttrConfig();
    readModeAttribute.setName("readMode");
    readModeAttribute.setValue("async");
    DdmAlterTableAttrConfig bulkLoadAttribute = new DdmAlterTableAttrConfig();
    bulkLoadAttribute.setName("bulkLoad");
    bulkLoadAttribute.setValue("true");
    String table = "test";
    alterTableChange.setTable(table);
    alterTableChange.setAttributeList(Arrays.asList(readModeAttribute, bulkLoadAttribute));

    List<String> errorMessages = alterTableChange.validate(new MockDatabase()).getErrorMessages();

    assertEquals(0, errorMessages.size());
  }

  @Test
  @DisplayName("Check statements - for all attributes")
  void checkStatementsAllAttributes() {
    DdmAlterTableAttrConfig readModeAttribute = new DdmAlterTableAttrConfig();
    readModeAttribute.setName("readMode");
    readModeAttribute.setValue("async");
    DdmAlterTableAttrConfig bulkLoadAttribute = new DdmAlterTableAttrConfig();
    bulkLoadAttribute.setName("bulkLoad");
    bulkLoadAttribute.setValue("true");
    String table = "test";
    alterTableChange.setTable(table);
    alterTableChange.setAttributeList(Arrays.asList(readModeAttribute, bulkLoadAttribute));

    SqlStatement[] statements = alterTableChange.generateStatements(new MockDatabase());
    Assertions.assertEquals(4, statements.length);
  }

  @Test
  @DisplayName("Check statements - for readMode attribute")
  void checkStatementsReadModeAttribute() {
    DdmAlterTableAttrConfig readModeAttribute = new DdmAlterTableAttrConfig();
    readModeAttribute.setName("readMode");
    readModeAttribute.setValue("async");
    String table = "test";
    alterTableChange.setTable(table);
    alterTableChange.setAttributeList(Collections.singletonList(readModeAttribute));

    SqlStatement[] statements = alterTableChange.generateStatements(new MockDatabase());
    Assertions.assertEquals(2, statements.length);
  }

  @Test
  @DisplayName("Check statements -for bulkLoad attribute")
  void checkStatementsBulkLoadAttribute() {
    DdmAlterTableAttrConfig bulkLoadAttribute = new DdmAlterTableAttrConfig();
    bulkLoadAttribute.setName("bulkLoad");
    bulkLoadAttribute.setValue("true");
    String table = "test";
    alterTableChange.setTable(table);
    alterTableChange.setAttributeList(Collections.singletonList(bulkLoadAttribute));

    SqlStatement[] statements = alterTableChange.generateStatements(new MockDatabase());
    Assertions.assertEquals(2, statements.length);
  }

  @Test
  @DisplayName("Check load")
  void checkLoad() throws Exception {
    List<ChangeSet> changeSets = DdmTest.loadChangeSets(DdmTest.TEST_ALTER_TABLE_API_FILE_NAME);

    Assertions.assertEquals(1, changeSets.size());
  }

  @Test
  @DisplayName("Confirmation Message")
  void confirmationMessage() {
    alterTableChange.setTable("test");

    assertEquals("Updated metadata for table [test]", alterTableChange.getConfirmationMessage());
  }
}