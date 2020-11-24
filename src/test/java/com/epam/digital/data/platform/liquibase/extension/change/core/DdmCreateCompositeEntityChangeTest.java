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

import com.epam.digital.data.platform.liquibase.extension.DdmTest;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLinkConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmNestedEntityConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import liquibase.change.Change;
import liquibase.change.ConstraintsConfig;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.core.MockDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DdmCreateCompositeEntityChangeTest {

  private DdmCreateCompositeEntityChange change;
  private ChangeSet changeSet;

  @BeforeEach
  public void setUp() {
    DatabaseChangeLog changeLog = new DatabaseChangeLog("path");
    changeSet = new ChangeSet(changeLog);

    change = new DdmCreateCompositeEntityChange();

    changeSet.addChange(change);

    DdmCreateTableChange tableChange = new DdmCreateTableChange();
    tableChange.setTableName("transaction");

    DdmColumnConfig column1 = new DdmColumnConfig();
    column1.setName("order_id");
    column1.setType("UUID");
    ConstraintsConfig constraint = new ConstraintsConfig();
    constraint.setForeignKeyName("forgn_name");
    constraint.setReferencedColumnNames("order_id");
    constraint.setReferencedTableName("order");
    column1.setConstraints(constraint);
    DdmColumnConfig column2 = new DdmColumnConfig();
    column2.setName("consent_id");
    column2.setType("UUID");
    ConstraintsConfig constraint1 = new ConstraintsConfig();
    constraint1.setForeignKeyName("forg_name");
    constraint1.setReferencedColumnNames("consent_id");
    constraint1.setReferencedTableName("consent_table");
    column2.setConstraints(constraint1);
    tableChange.addColumn(column1);
    tableChange.addColumn(column2);

    changeSet.addChange(tableChange);
    changeLog.addChangeSet(changeSet);
  }

  @Test
  public void checkStatements() {
    change.setName("name");
    change.setNestedEntities(createEntities());
    SqlStatement[] statements = change.generateStatements(new MockDatabase());
    Assertions.assertEquals(3, statements.length);
    Assertions.assertTrue(statements[0] instanceof RawSqlStatement);
    Assertions.assertTrue(statements[1] instanceof RawSqlStatement);
    Assertions.assertTrue(statements[2] instanceof RawSqlStatement);
    Assertions.assertEquals(
        "insert into ddm_liquibase_metadata(change_type, change_name, attribute_name, attribute_value) values ('nested', 'name', 'transaction', 'order_id');\n"
            + "\n", ((RawSqlStatement) statements[0]).getSql());
    Assertions.assertEquals(
        "insert into ddm_liquibase_metadata(change_type, change_name, attribute_name, attribute_value) values ('nested', 'name', 'transaction', 'consent_id');\n"
            + "\n", ((RawSqlStatement) statements[1]).getSql());
    Assertions.assertEquals(
        "insert into ddm_liquibase_metadata(change_type, change_name, attribute_name, attribute_value) values ('nested', 'name', 'order', 'application_id');\n"
            + "\n", ((RawSqlStatement) statements[2]).getSql());
  }

  @Test
  public void checkLoad() throws Exception {
    List<ChangeSet> changeSets = DdmTest.loadChangeSets(
        DdmTest.TEST_COMPOSITE_NESTED_ENTITY_FILE_NAME);
    Change change = changeSets.get(0).getChanges().get(0);

    Assertions.assertEquals(1, changeSets.size());
    Assertions.assertEquals(1, changeSets.get(0).getChanges().size());
    Assertions.assertTrue(change instanceof DdmCreateCompositeEntityChange);

    List<DdmNestedEntityConfig> nestedEntities = ((DdmCreateCompositeEntityChange) change).getNestedEntities();

    Assertions.assertEquals("composite", ((DdmCreateCompositeEntityChange) change).getName());
    Assertions.assertEquals(4, nestedEntities.size());
    Assertions.assertEquals("transaction", nestedEntities.get(0).getTable());
    Assertions.assertEquals(2, nestedEntities.get(0).getLinkConfig().size());
    Assertions.assertEquals("order", nestedEntities.get(1).getTable());
    Assertions.assertEquals(1, nestedEntities.get(1).getLinkConfig().size());
    Assertions.assertEquals("application", nestedEntities.get(2).getTable());
    Assertions.assertEquals(0, nestedEntities.get(2).getLinkConfig().size());
    Assertions.assertEquals("consent_table", nestedEntities.get(3).getTable());
    Assertions.assertEquals(0, nestedEntities.get(3).getLinkConfig().size());
  }

  @Test
  public void validateChange() {
    change.setName("name");
    change.setNestedEntities(createEntities());

    DdmCreateTableChange tableChange1 = new DdmCreateTableChange();
    tableChange1.setTableName("order");

    DdmColumnConfig column2 = new DdmColumnConfig();
    column2.setName("application_id");
    column2.setType("UUID");
    ConstraintsConfig constraint1 = new ConstraintsConfig();
    constraint1.setForeignKeyName("frgn_name");
    constraint1.setReferencedColumnNames("application_id");
    constraint1.setReferencedTableName("application");
    column2.setConstraints(constraint1);

    DdmColumnConfig column3 = new DdmColumnConfig();
    column3.setName("random_id");
    column3.setType("UUID");
    ConstraintsConfig constraint2 = new ConstraintsConfig();
    constraint2.setForeignKeyName("forgn_name_rand");
    constraint2.setReferencedColumnNames("random_id");
    constraint2.setReferencedTableName("random");
    column3.setConstraints(constraint2);

    tableChange1.addColumn(column2);
    tableChange1.addColumn(column3);

    changeSet.addChange(tableChange1);

    DdmCreateTableChange tableChange2 = new DdmCreateTableChange();
    tableChange2.setTableName("application");
    DdmCreateTableChange tableChange3 = new DdmCreateTableChange();
    tableChange3.setTableName("consent_table");

    changeSet.addChange(tableChange2);
    changeSet.addChange(tableChange3);

    Assertions.assertEquals(0, change.validate(new MockDatabase()).getErrorMessages().size());
  }

  @Test
  public void validateChangeErrorNoTables() {
    change.setName("name");
    change.setNestedEntities(createEntities());

    DdmCreateTableChange tableChange1 = new DdmCreateTableChange();
    tableChange1.setTableName("order");

    DdmColumnConfig column2 = new DdmColumnConfig();
    column2.setName("application_id");
    column2.setType("UUID");
    ConstraintsConfig constraint1 = new ConstraintsConfig();
    constraint1.setForeignKeyName("forgn_name");
    constraint1.setReferencedColumnNames("application_id");
    constraint1.setReferencedTableName("application");
    column2.setConstraints(constraint1);
    tableChange1.addColumn(column2);

    changeSet.addChange(tableChange1);

    Assertions.assertEquals("Missing required tables: [application, consent_table]",
        change.validate(new MockDatabase()).getErrorMessages().get(0));
  }

  @Test
  public void validateChangeErrorNoRelations() {
    change.setName("name");
    change.setNestedEntities(createEntities());

    DdmCreateTableChange tableChange1 = new DdmCreateTableChange();
    tableChange1.setTableName("order");

    changeSet.addChange(tableChange1);

    DdmCreateTableChange tableChange2 = new DdmCreateTableChange();
    tableChange2.setTableName("application");
    DdmCreateTableChange tableChange3 = new DdmCreateTableChange();
    tableChange3.setTableName("consent_table");

    changeSet.addChange(tableChange2);
    changeSet.addChange(tableChange3);

    Assertions.assertEquals("Not enough required relations",
        change.validate(new MockDatabase()).getErrorMessages().get(0));
  }

  private List<DdmNestedEntityConfig> createEntities() {
    List<DdmNestedEntityConfig> entities = new ArrayList<>();

    DdmNestedEntityConfig entity = new DdmNestedEntityConfig();
    entity.setTable("transaction");
    DdmLinkConfig link = new DdmLinkConfig();
    link.setColumn("order_id");
    link.setEntity("order");
    DdmLinkConfig link1 = new DdmLinkConfig();
    link1.setColumn("consent_id");
    link1.setEntity("consent");
    entity.setLinkConfig(Arrays.asList(link, link1));
    entities.add(entity);
    DdmNestedEntityConfig entity1 = new DdmNestedEntityConfig();
    entity1.setTable("order");
    entity1.setName("order");
    DdmLinkConfig link2 = new DdmLinkConfig();
    link2.setColumn("application_id");
    link2.setEntity("app");
    entity1.setLinkConfig(Collections.singletonList(link2));
    entities.add(entity1);
    DdmNestedEntityConfig entity2 = new DdmNestedEntityConfig();
    entity2.setTable("application");
    entity2.setName("app");
    entity2.setLinkConfig(Collections.emptyList());
    entities.add(entity2);
    DdmNestedEntityConfig entity3 = new DdmNestedEntityConfig();
    entity3.setTable("consent_table");
    entity3.setName("consent");
    entity3.setLinkConfig(Collections.emptyList());
    entities.add(entity3);

    return entities;
  }
}