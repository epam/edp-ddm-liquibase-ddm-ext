package com.epam.digital.data.platform.liquibase.extension.sqlgenerator.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

import com.epam.digital.data.platform.liquibase.extension.DdmConstants;
import com.epam.digital.data.platform.liquibase.extension.change.DdmColumnConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmConditionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmCteConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmFunctionConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmJoinConfig;
import com.epam.digital.data.platform.liquibase.extension.change.DdmTableConfig;
import com.epam.digital.data.platform.liquibase.extension.statement.core.DdmCreateSearchConditionStatement;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class DdmCreateSearchConditionGenerator extends AbstractSqlGenerator<DdmCreateSearchConditionStatement> {

    @Override
    public ValidationErrors validate(DdmCreateSearchConditionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("name", statement.getName());
        return validationErrors;
    }

    private String getUnusedTable(List<DdmTableConfig> tables, String leftAlias, String rightAlias) {
        for (DdmTableConfig table : tables) {
            if (!table.getUsedInSQLClause() && (table.getAlias().equals(leftAlias) || table.getAlias().equals(rightAlias))) {
                table.setUsedInSQLClause(true);
                return table.getName() + " AS " + table.getAlias();
            }
        }
        return "";
    }

    private StringBuilder generateIndexSql(String name, List<DdmTableConfig> tables) {
        StringBuilder buffer = new StringBuilder();

        for (DdmTableConfig table : tables) {
            for (DdmColumnConfig column : table.getColumns()) {
                if (Objects.nonNull(column.getSearchType())) {
                    buffer.append("\n\n");
                    buffer.append("CREATE INDEX ");
                    buffer.append(DdmConstants.PREFIX_INDEX);
                    buffer.append("$");
                    buffer.append(name);
                    buffer.append("$_");
                    buffer.append(table.getName());
                    buffer.append("_");
                    buffer.append(column.getName());
                    buffer.append(" ON ");
                    buffer.append(table.getName());
                    buffer.append("(");

                    String columnFormat = column.getName();

                    if (column.getSearchType().equalsIgnoreCase(DdmConstants.ATTRIBUTE_CONTAINS) || column.getSearchType().equalsIgnoreCase(DdmConstants.ATTRIBUTE_STARTS_WITH)) {
                        columnFormat += " ";

                        if (column.getType().equalsIgnoreCase(DdmConstants.TYPE_CHAR)) {
                            columnFormat += "bp";
                        }

                        columnFormat += column.getType().toLowerCase();
                        columnFormat += "_pattern_ops";
                    }

                    buffer.append(columnFormat);
                    buffer.append(");");
                }
            }
        }

        return buffer;
    }

    private StringBuilder generateSelectSql(List<DdmTableConfig> tables, List<DdmJoinConfig> joins, List<DdmConditionConfig> conditions) {
        StringBuilder buffer = new StringBuilder();
        List<String> columns = new ArrayList<>();
        List<String> orderColumns = new ArrayList<>();
        List<String> groupColumns = new ArrayList<>();
        boolean hasFunctions = false;

        buffer.append("SELECT ");

        for (DdmTableConfig table : tables) {
            for (DdmColumnConfig column : table.getColumns()) {
                columns.add((table.hasAlias() ? table.getAlias() + "." : "") +
                    column.getName() +
                    (column.hasAlias() ? " AS " + column.getAlias() : ""));

                groupColumns.add((table.hasAlias() ? table.getAlias() + "." : "") + column.getName());

                if (Objects.nonNull(column.getSorting())) {
                    orderColumns.add((table.hasAlias() ? table.getAlias() + "." : "") +
                            column.getName() +
                            (column.getSorting().equalsIgnoreCase(DdmConstants.ATTRIBUTE_DESC) ? " " + column.getSorting().toUpperCase() : ""));
                }
            }

            for (DdmFunctionConfig function : table.getFunctions()) {
                hasFunctions = true;
                columns.add(function.getName().toUpperCase() + "(" +
                        (function.hasTableAlias() ? function.getTableAlias() + "." : "") +
                        function.getColumnName() +
                        (function.hasParameter() ? ", " + function.getParameter() : "") +
                        ") AS " + function.getAlias());

                groupColumns.remove((function.hasTableAlias() ? function.getTableAlias() + "." : "") +
                        function.getColumnName());

                columns.remove((function.hasTableAlias() ? function.getTableAlias() + "." : "") +
                        function.getColumnName());
            }
        }

        buffer.append(String.join(", ", columns));

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

        if (Objects.nonNull(conditions) && conditions.size() > 0) {
            buffer.append(" WHERE ");
            buffer.append(generateConditionSql(conditions, false));
        }

        if (orderColumns.size() > 0) {
            buffer.append(" ORDER BY ");
            buffer.append(String.join(", ", orderColumns));
        }

        if ((groupColumns.size() > 0) && hasFunctions) {
            buffer.append(" GROUP BY ");
            buffer.append(String.join(", ", groupColumns));
        }

        return buffer;
    }

    @Override
    public Sql[] generateSql(DdmCreateSearchConditionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("CREATE OR REPLACE VIEW ");
        buffer.append(statement.getViewName());
        buffer.append(" AS ");

        if (statement.getCtes().size() > 0) {
            List<String> cteSelectSql = new ArrayList<>();
            buffer.append("WITH ");

            for (DdmCteConfig cte : statement.getCtes()) {
                cteSelectSql.add(cte.getName() + " AS (" +
                        generateSelectSql(cte.getTables(), cte.getJoins(), cte.getConditions()) + ")");
            }

            buffer.append(String.join(", ", cteSelectSql)).append(" ");
        }

        buffer.append(generateSelectSql(statement.getTables(), statement.getJoins(), statement.getConditions()));

        buffer.append(";");

        if (Boolean.TRUE.equals(statement.getIndexing())) {
            buffer.append(generateIndexSql(statement.getName(), statement.getTables()));
        }

        return new Sql[] {
                new UnparsedSql(buffer.toString())
        };
    }

    private StringBuilder generateConditionSql(List<DdmConditionConfig> conditions, boolean hasInternalParenthesis) {
        StringBuilder buffer = new StringBuilder();

        if (Objects.isNull(conditions)) {
            return buffer;
        }

        boolean firstCondition = true;

        for (DdmConditionConfig condition : conditions) {
            boolean hasExternalParenthesis = ((conditions.size() > 1) && Objects.nonNull(condition.getConditions()));

            if (Objects.nonNull(condition.getLogicOperator())) {
                buffer.append(" " + condition.getLogicOperator() + " ");
            }

            if (hasExternalParenthesis) {
                buffer.append("(");
            }

            if (firstCondition) {
                if (hasInternalParenthesis) {
                    buffer.append("(");
                }

                firstCondition = false;
            }

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

            boolean needParenthesis = (Objects.nonNull(condition.getConditions()) && condition.getConditions().size() > 1);
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
