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

import com.epam.digital.data.platform.liquibase.extension.change.core.DdmArchiveAffectableChange;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import liquibase.change.Change;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;

/**
 * This class stores a copy of information about previously executed ChangeSets and has methods 
 * for working with the current ChangeLog
 */
public class DdmRanChangeSetsKeeper {
    
    private DdmRanChangeSetsKeeper() {
    }

    private static Map<String, RanChangeSet> ranChangeSets;

    public static boolean isChangeFirst(Database database, DdmArchiveAffectableChange change) {
        Map<String, RanChangeSet> ranChangeSets = getRanChangeSets(database);
        List<ChangeSet> xmlChangeSets = change.getChangeSet().getChangeLog().getChangeSets();

        for (ChangeSet xmlChangeSet : xmlChangeSets) {
            if (xmlChangeSet.equals(change.getChangeSet())) {
                for(Change c : xmlChangeSet.getChanges()) {
                    if(c.equals(change)) {
                        break;
                    }
                    if (isTheSameTableModifyingChange(c, change)) {
                        return false;
                    }
                }
                break;
            }

            if (shouldSkip(ranChangeSets, xmlChangeSet)) {
                continue;
            }

            for (Change c : xmlChangeSet.getChanges()) {
                if (isTheSameTableModifyingChange(c, change)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isTheSameTableModifyingChange(Change c, DdmArchiveAffectableChange change) {
        return DdmArchiveAffectableChange.class.isAssignableFrom(c.getClass())
            && ((DdmArchiveAffectableChange) c).getTableName().equals(change.getTableName());
    }

    private static Map<String, RanChangeSet> getRanChangeSets(Database database) {
        if (ranChangeSets == null) {
            try {
                ranChangeSets = Collections.unmodifiableMap(database.getRanChangeSetList().stream()
                    .collect(Collectors.toMap(RanChangeSet::toString, Function.identity())));
            } catch (DatabaseException e) {
                throw new UnexpectedLiquibaseException(e);
            }
        }
        return ranChangeSets;
    }

    private static boolean shouldSkip(Map<String, RanChangeSet> ranChangeSets, ChangeSet changeSet) {
        RanChangeSet foundChangeSet = ranChangeSets.get(changeSet.toString(false));
        if (foundChangeSet == null) {
            for (RanChangeSet ranChangeSet : ranChangeSets.values()) {
                if (ranChangeSet.isSameAs(changeSet)) {
                    foundChangeSet = ranChangeSet;
                    break;
                }
            }
        }
        boolean shouldRun = (foundChangeSet == null || changeSet.shouldRunOnChange()
            || changeSet.shouldAlwaysRun()) && !changeSet.isIgnore();

        return !shouldRun;
    }
}
