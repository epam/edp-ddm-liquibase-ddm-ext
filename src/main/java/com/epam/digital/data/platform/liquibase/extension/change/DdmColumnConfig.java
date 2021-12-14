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
import liquibase.change.ColumnConfig;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.util.StringUtil;

public class DdmColumnConfig extends ColumnConfig {

    private String collation;
    private String alias;
    private String sorting;
    private String searchType;
    private Boolean read;
    private Boolean update;
    private Boolean returning;
    private String classify;

    public DdmColumnConfig() {
        super();
    }

    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        super.load(parsedNode, resourceAccessor);

        setAlias(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_ALIAS, String.class));
        setSorting(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_SORTING, String.class));
        setSearchType(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_SEARCH_TYPE, String.class));
        setRead(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_READ, Boolean.class));
        setUpdate(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_UPDATE, Boolean.class));
        setReturning(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_RETURNING, Boolean.class));
        setClassify(parsedNode.getChildValue(null, DdmConstants.ATTRIBUTE_CLASSIFY, String.class));
    }

    public String getAliasOrName() {
        return hasAlias() ? getAlias() : getName();
    }

    public String getNameAsAlias() {
        return getName() + (hasAlias() ? " AS " + getAlias() : "") ;
    }

    public boolean hasAlias() { return !StringUtil.isEmpty(getAlias()); }

    public Boolean getRoleCanRead() {
        return getRead();
    }

    public Boolean getRoleCanUpdate() {
        return getUpdate();
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getSorting() {
        return sorting;
    }

    public void setSorting(String sorting) {
        this.sorting = sorting;
    }

    public String getSearchType() {
        return searchType;
    }

    public void setSearchType(String searchType) {
        this.searchType = searchType;
    }

    public Boolean getRead() {
        return read;
    }

    public void setRead(Boolean read) {
        this.read = read;
    }

    public Boolean getUpdate() {
        return update;
    }

    public void setUpdate(Boolean update) {
        this.update = update;
    }

    public Boolean getReturning() {
        return returning;
    }

    public void setReturning(Boolean returning) {
        this.returning = returning;
    }

    public String getClassify() {
        return classify;
    }

    public void setClassify(String classify) {
        this.classify = classify;
    }
}
