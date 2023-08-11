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

package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.DdmPair;
import com.epam.digital.data.platform.liquibase.extension.DdmUtils;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmConditionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmCteConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmFunctionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmLogicOperatorConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateAbstractViewStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DdmCreateAbstractViewGenerator extends AbstractSqlGenerator<DdmCreateAbstractViewStatement> {

    public static final String EMPTY_STRING = "";

    @Override
    public ValidationErrors validate(DdmCreateAbstractViewStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", statement.getName());

        String cteErrorMessage = getErrorMessageForCteValidator(statement);
        if(cteErrorMessage != null) {
            validationErrors.addError("CTE has incorrect format: " + cteErrorMessage);
        }
        return validationErrors;
    }

    private String getErrorMessageForCteValidator(DdmCreateAbstractViewStatement statement) {
        String result = null;
        try {
            for (DdmTableConfig table : statement.getTables()) {
                for (DdmColumnConfig column : table.getColumns()) {
                    if (column.getSearchType() != null) {
                        getTableColumnPairForCteColumn(statement, table.getName(), column.getName());
                    }
                }
            }
        } catch (RuntimeException e) {
            result = e.getMessage();
        }
        return result;
    }

    private String getUnusedTable(List<DdmTableConfig> tables, String leftAlias, String rightAlias) {
        for (DdmTableConfig table : tables) {
            if (!table.getUsedInSQLClause() && (table.getAlias().equals(leftAlias) || table.getAlias().equals(rightAlias))) {
                table.setUsedInSQLClause(true);
                return table.getName() + " AS " + table.getAlias();
            }
        }
        return EMPTY_STRING;
    }

    private DdmPair getPair(Map<String, DdmCteConfig> ctes, String tableName, String columnName) {
        if (ctes.containsKey(tableName)) {
            for (DdmTableConfig cteTable : ctes.get(tableName).getTables()) {
                for (DdmColumnConfig cteColumn : cteTable.getColumns()) {
                    if (cteColumn.getAliasOrName().equalsIgnoreCase(columnName)) {
                        tableName = cteTable.getName();
                        columnName = cteColumn.getName();
                        return getPair(ctes, tableName, columnName);
                    }
                }
                for (DdmFunctionConfig cteFunction : cteTable.getFunctions()) {
                    if (cteFunction.getAlias().equalsIgnoreCase(columnName)) {
                        tableName = cteTable.getName();
                        columnName = cteFunction.getColumnName();
                        return getPair(ctes, tableName, columnName);
                    }
                }
            }
            throw new RuntimeException(columnName + " column was not found in the table " + tableName);
        }
        return new DdmPair(tableName, columnName);
    }

    private DdmPair getTableColumnPairForCteColumn(DdmCreateAbstractViewStatement statement, String tableName, String columnName) {

        Map<String, DdmCteConfig> cteMap = statement.getCtes().stream()
            .collect(Collectors.toMap(DdmCteConfig::getName, Function.identity()));

        return getPair(cteMap, tableName, columnName);
    }

    private StringBuilder generateIndexSql(DdmCreateAbstractViewStatement statement) {
        StringBuilder buffer = new StringBuilder();

        for (DdmTableConfig table : statement.getTables()) {
            for (DdmColumnConfig column : table.getColumns()) {
                if (column.getSearchType() != null) {
                    DdmPair pair = getTableColumnPairForCteColumn(statement, table.getName(), column.getName());

                    String tableName = pair.getKey();
                    String columnName = pair.getValue();

                    buffer.append("\n\n");
                    buffer.append("CREATE INDEX IF NOT EXISTS ");
                    buffer.append(DdmConstants.PREFIX_INDEX);
                    buffer.append(tableName);
                    buffer.append("__");
                    buffer.append(columnName);

                    buffer.append(" ON ");
                    buffer.append(tableName);

                    if (column.getSearchType().equalsIgnoreCase(DdmConstants.ATTRIBUTE_CONTAINS)) {
                        buffer.append(" USING GIN ");
                    }

                    buffer.append("(");

                    boolean isColumnCastable = DdmUtils.isColumnAvailableForCasting(column);
                    if (column.getSearchType().equalsIgnoreCase(DdmConstants.ATTRIBUTE_CONTAINS)) {
                        columnName += " gin_trgm_ops";
                    } else if (column.getSearchType().equalsIgnoreCase(DdmConstants.ATTRIBUTE_STARTS_WITH) ||
                            column.getSearchType().equalsIgnoreCase(DdmConstants.ATTRIBUTE_STARTS_WITH_ARRAY)) {
                        columnName += " ";

                        if (column.getType().equalsIgnoreCase(DdmConstants.TYPE_CHAR)) {
                            columnName += "bp";
                        }

                        columnName += column.getType().toLowerCase();
                        columnName += "_pattern_ops";
                    } else if (isColumnCastable) {
                        buffer.append("lower(cast(");
                    }

                    buffer.append(columnName);
                    if (isColumnCastable) {
                        buffer.append(" as varchar))");
                    }
                    buffer.append(");");
                }
            }
        }

        return buffer;
    }

    private StringBuilder generateSelectSql(List<DdmTableConfig> tables, List<DdmJoinConfig> joins, List<DdmConditionConfig> conditions) {
        StringBuilder buffer = new StringBuilder();
        List<String> viewResultColumns = new ArrayList<>();
        List<String> orderColumns = new ArrayList<>();
        List<String> groupColumns = new ArrayList<>();
        boolean hasNonWindowFunctions = false;

        buffer.append("SELECT ");

        for (DdmTableConfig table : tables) {
            String tableAlias = table.hasAlias() ? table.getAlias() + "." : "";
            processTableColumns(table.getColumns(), viewResultColumns, orderColumns, groupColumns, tableAlias);
            if (Objects.nonNull(table.getTableLogicOperator())) {
                processNestedColumnsFromLogicOperators(table.getTableLogicOperator().getLogicOperators(), viewResultColumns,
                        orderColumns, groupColumns, tableAlias);
            }

            for (DdmFunctionConfig function : table.getFunctions()) {
                hasNonWindowFunctions = function.getWindow() == null;
                StringBuilder functionContent = new StringBuilder();
                functionContent.append(function.getName().toUpperCase()).append("(");
                if (!function.getName().equalsIgnoreCase(DdmConstants.ATTRIBUTE_FUNCTION_ROW_NUMBER)) {
                    functionContent
                        .append(function.hasTableAlias() ? function.getTableAlias() + "." : "")
                        .append(function.getColumnName())
                        .append(function.hasParameter() ? ", " + function.getParameter() : "");
                }
                functionContent.append(") ");
                if (function.getWindow() != null) {
                    functionContent.append("OVER (");
                    if (!function.getWindow().isEmpty()) {
                        functionContent.append(function.getWindow());
                    }
                    functionContent.append(") ");
                }
                viewResultColumns.add(functionContent + "AS " + function.getAlias());

                groupColumns.remove((function.hasTableAlias() ? function.getTableAlias() + "." : "") +
                        function.getColumnName());

                viewResultColumns.remove((function.hasTableAlias() ? function.getTableAlias() + "." : "") +
                        function.getColumnName());
            }
        }

        buffer.append(String.join(", ", viewResultColumns));

        DdmTableConfig firstTable = tables.get(0);
        buffer.append(" FROM ");
        buffer.append(firstTable.getName());

        if (firstTable.hasAlias()) {
            buffer.append(" AS ");
            buffer.append(firstTable.getAlias());
        }

        firstTable.setUsedInSQLClause(true);

        for (DdmJoinConfig join : joins) {
            buffer.append(" ");
            buffer.append(join.getType().toUpperCase());
            buffer.append(" JOIN ");
            buffer.append(getUnusedTable(tables, join.getLeftAlias(), join.getRightAlias()));
            buffer.append(" ON ");

            ListIterator<String> columnIterator = join.getLeftColumns().listIterator();
            while (columnIterator.hasNext()) {
                String column = columnIterator.next();
                buffer.append("(");
                buffer.append(join.getLeftAlias());
                buffer.append(".");
                buffer.append(column);
                buffer.append(" = ");
                buffer.append(join.getRightAlias());
                buffer.append(".");
                buffer.append(join.getRightColumns().get(columnIterator.previousIndex()));
                buffer.append(")");

                if (columnIterator.hasNext()) {
                    buffer.append(" AND ");
                }
            }

            buffer.append(generateConditionSql(join.getConditions(), false));
        }

        if (conditions != null && !conditions.isEmpty()) {
            buffer.append(" WHERE ");
            buffer.append(generateConditionSql(conditions, false));
        }

        if (!groupColumns.isEmpty() && hasNonWindowFunctions) {
            buffer.append(" GROUP BY ");
            buffer.append(String.join(", ", groupColumns));
        }

        if (!orderColumns.isEmpty()) {
            buffer.append(" ORDER BY ");
            buffer.append(String.join(", ", orderColumns));
        }

        return buffer;
    }

    private void processTableColumns(List<DdmColumnConfig> columnObjects, List<String> viewResultColumns,
                                     List<String> orderColumns, List<String> groupColumns, String tableAlias) {
        for (DdmColumnConfig column : columnObjects) {
            viewResultColumns.add(tableAlias + column.getName() + (column.hasAlias() ? " AS " + column.getAlias() : ""));
            groupColumns.add(tableAlias + column.getName());

            if (column.getSorting() != null) {
                orderColumns.add(tableAlias + column.getName() +
                        (column.getSorting().equalsIgnoreCase(DdmConstants.ATTRIBUTE_DESC) ? " " + column.getSorting().toUpperCase() : ""));
            }
        }
    }

    private void processNestedColumnsFromLogicOperators(List<DdmLogicOperatorConfig> logicOperators,
                                                        List<String> viewResultColumns, List<String> orderColumns,
                                                        List<String> groupColumns, String tableAlias) {
        for (DdmLogicOperatorConfig logicOperator : logicOperators) {
            processTableColumns(logicOperator.getColumns(), viewResultColumns, orderColumns, groupColumns, tableAlias);
            if (Objects.nonNull(logicOperator.getLogicOperators()) && !logicOperator.getLogicOperators().isEmpty()) {
                processNestedColumnsFromLogicOperators(logicOperator.getLogicOperators(), viewResultColumns, orderColumns, groupColumns, tableAlias);
            }
        }
    }

    @Override
    public Sql[] generateSql(DdmCreateAbstractViewStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("CREATE OR REPLACE VIEW ");
        buffer.append(statement.getViewName());
        buffer.append(" AS ");

        if (!statement.getCtes().isEmpty()) {
            buffer.append("WITH ");

            String cteSelectSql = statement.getCtes().stream().map(cte -> cte.getName() + " AS (" +
                    generateSelectSql(cte.getTables(), cte.getJoins(), cte.getConditions()) + ")")
                .collect(Collectors.joining(", "));

            buffer.append(cteSelectSql).append(" ");
        }

        buffer.append(generateSelectSql(statement.getTables(), statement.getJoins(), statement.getConditions()));
        buffer.append(";");

        if (Boolean.TRUE.equals(statement.getIndexing())) {
            buffer.append(generateIndexSql(statement));
        }

        return new Sql[]{ new UnparsedSql(buffer.toString()) };
    }

    private StringBuilder generateConditionSql(List<DdmConditionConfig> conditions, boolean hasInternalParenthesis) {
        StringBuilder buffer = new StringBuilder();

        if (conditions == null) {
            return buffer;
        }

        boolean firstCondition = true;

        for (DdmConditionConfig condition : conditions) {
            boolean hasExternalParenthesis = ((conditions.size() > 1) && condition.getConditions() != null);

            if (condition.getLogicOperator() != null) {
                buffer.append(" ").append(condition.getLogicOperator()).append(" ");
            }

            if (hasExternalParenthesis) {
                buffer.append("(");
            }

            if (firstCondition && hasInternalParenthesis) {
                buffer.append("(");
            }
            firstCondition = false;

            buffer.append("(");

            if (condition.hasTableAlias()) {
                buffer.append(condition.getTableAlias());
                buffer.append(".");
            }

            buffer.append(condition.getColumnName());

            switch (condition.getOperator()) {
                case DdmConstants.OPERATOR_EQ:
                    buffer.append(" = ");
                    break;
                case DdmConstants.OPERATOR_NE:
                    buffer.append(" <> ");
                    break;
                case DdmConstants.OPERATOR_GT:
                    buffer.append(" > ");
                    break;
                case DdmConstants.OPERATOR_GE:
                    buffer.append(" >= ");
                    break;
                case DdmConstants.OPERATOR_LT:
                    buffer.append(" < ");
                    break;
                case DdmConstants.OPERATOR_LE:
                    buffer.append(" <= ");
                    break;
                case DdmConstants.OPERATOR_IN:
                    buffer.append(" IN (");
                    buffer.append(condition.getValue());
                    buffer.append(")");
                    break;
                case DdmConstants.OPERATOR_NOT_IN:
                    buffer.append(" NOT IN (");
                    buffer.append(condition.getValue());
                    buffer.append(")");
                    break;
                case DdmConstants.OPERATOR_IS_NULL:
                    buffer.append(" IS");

                    if (condition.getValue().equals(DdmConstants.ATTRIBUTE_FALSE)) {
                        buffer.append(" NOT");
                    }

                    buffer.append(" NULL");
                    break;
                case DdmConstants.OPERATOR_SIMILAR:
                    buffer.append(" ~ ");
                    break;
                case DdmConstants.OPERATOR_LIKE:
                    buffer.append(" LIKE ");
                    break;
            }

            if (!Arrays.asList(DdmConstants.OPERATOR_IS_NULL, DdmConstants.OPERATOR_IN, DdmConstants.OPERATOR_NOT_IN).contains(condition.getOperator())) {
                buffer.append(condition.getValue());
            }

            buffer.append(")");

            boolean needParenthesis = (condition.getConditions() != null && condition.getConditions().size() > 1);
            buffer.append(generateConditionSql(condition.getConditions(), needParenthesis));

            if (hasExternalParenthesis) {
                buffer.append(")");
            }

        }

        if (hasInternalParenthesis) {
            buffer.append(")");
        }

        return buffer;
    }
}
