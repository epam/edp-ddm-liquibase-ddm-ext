package com.epam.digital.data.platform.liquibase.extension;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.RuntimeEnvironment;
import liquibase.changelog.ChangeLogIterator;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.filter.ChangeSetFilterResult;
import liquibase.changelog.visitor.ChangeSetVisitor;
import liquibase.database.Database;
import liquibase.database.core.MockDatabase;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;

public class DdmTest {

    public static final String TEST_CREATE_ENUM_TYPE_FILE_NAME = "createEnumTypeChangelog.xml";
    public static final String TEST_CREATE_COMPOSITE_TYPE_FILE_NAME = "createCompositeTypeChangelog.xml";
    public static final String TEST_CREATE_SEARCH_CONDITION_FILE_NAME = "createSearchConditionChangelog.xml";
    public static final String TEST_CREATE_M2M_FILE_NAME = "createMany2ManyChangelog.xml";
    public static final String TEST_RBAC_FILE_NAME = "rbacChangelog.xml";
    public static final String TEST_PARTIAL_UPDATE_FILE_NAME = "partialUpdateChangelog.xml";
    public static final String TEST_MAKE_OBJECT_FILE_NAME = "makeObjectChangelog.xml";
    public static final String TEST_CREATE_TABLE_FILE_NAME = "createTableChangelog.xml";
    public static final String TEST_GRANT_ALL_FILE_NAME = "grantAllChangelog.xml";
    public static final String TEST_GRANT_FILE_NAME = "grantChangelog.xml";
    public static final String TEST_REVOKE_ALL_FILE_NAME = "revokeAllChangelog.xml";
    public static final String TEST_REVOKE_FILE_NAME = "revokeChangelog.xml";

    private DdmTest() {}

    public static List<ChangeSet> loadChangeSets(String fileXML) throws Exception {
        XMLChangeLogSAXParser xmlParser = new XMLChangeLogSAXParser();
        DdmResourceAccessor resourceAccessor = new DdmResourceAccessor();
        DatabaseChangeLog changeLog = xmlParser.parse(fileXML,
            new ChangeLogParameters(), resourceAccessor);

        final List<ChangeSet> changeSets = new ArrayList<>();

        new ChangeLogIterator(changeLog).run(new ChangeSetVisitor() {
            @Override
            public Direction getDirection() {
                return Direction.FORWARD;
            }

            @Override
            public void visit(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database, Set<ChangeSetFilterResult> filterResults) {
                changeSets.add(changeSet);
            }
        }, new RuntimeEnvironment(new MockDatabase(), new Contexts(), new LabelExpression()));

        return changeSets;
    }
}
