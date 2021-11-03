package com.epam.digital.data.platform.liquibase.extension;

public class DdmConstants {

    public static final String PARAMETERS_FILE_NAME = "parameters.xml";

    public static final String FIELD_CLASSIFICATION = "dcm_classification";

    public static final String ATTRIBUTE_NAME = "name";
    public static final String ATTRIBUTE_TYPE = "type";
    public static final String ATTRIBUTE_SCOPE = "scope";
    public static final String ATTRIBUTE_UNIQUE_WITH_PRIMARY_KEY = "uniqueWithPrimaryKey";
    public static final String ATTRIBUTE_NULLABLE = "nullable";
    public static final String ATTRIBUTE_DEFAULT_VALUE_COMPUTED = "defaultValueComputed";
    public static final String ATTRIBUTE_COLUMN = "column";
    public static final String ATTRIBUTE_COLUMN_NAME = "columnName";
    public static final String ATTRIBUTE_LABEL = "label";
    public static final String ATTRIBUTE_TRANSLATION = "translation";
    public static final String ATTRIBUTE_COLLATION = "collation";
    public static final String ATTRIBUTE_LIMIT = "limit";
    public static final String ATTRIBUTE_TABLE = "table";
    public static final String ATTRIBUTE_CTE = "cte";
    public static final String ATTRIBUTE_ALIAS = "alias";
    public static final String ATTRIBUTE_TABLE_ALIAS = "tableAlias";
    public static final String ATTRIBUTE_SEARCH_COLUMN = "searchColumn";
    public static final String ATTRIBUTE_RETURNING = "returning";
    public static final String ATTRIBUTE_SORTING = "sorting";
    public static final String ATTRIBUTE_EQUAL = "equal";
    public static final String ATTRIBUTE_CONTAINS = "contains";
    public static final String ATTRIBUTE_STARTS_WITH = "startsWith";
    public static final String ATTRIBUTE_EQUAL_COLUMN = "equalColumn";
    public static final String ATTRIBUTE_CONTAINS_COLUMN = "containsColumn";
    public static final String ATTRIBUTE_STARTS_WITH_COLUMN = "startsWithColumn";
    public static final String ATTRIBUTE_JOIN = "join";
    public static final String ATTRIBUTE_WHERE = "where";
    public static final String ATTRIBUTE_LEFT = "left";
    public static final String ATTRIBUTE_RIGHT = "right";
    public static final String ATTRIBUTE_DESC = "desc";
    public static final String ATTRIBUTE_ALL = "all";
    public static final String ATTRIBUTE_TRUE = "true";
    public static final String ATTRIBUTE_FALSE = "false";
    public static final String ATTRIBUTE_SEARCH_TYPE = "searchType";
    public static final String ATTRIBUTE_OPERATOR = "operator";
    public static final String ATTRIBUTE_VALUE = "value";
    public static final String ATTRIBUTE_CONDITION = "condition";
    public static final String ATTRIBUTE_LOGIC_OPERATOR = "logicOperator";
    public static final String ATTRIBUTE_PARAMETER = "parameter";
    public static final String ATTRIBUTE_FUNCTION = "function";
    public static final String ATTRIBUTE_FUNCTION_STRING_AGG = "string_agg";
    public static final String ATTRIBUTE_ROLE = "role";
    public static final String ATTRIBUTE_INSERT = "insert";
    public static final String ATTRIBUTE_UPDATE = "update";
    public static final String ATTRIBUTE_DELETE = "delete";
    public static final String ATTRIBUTE_READ = "read";
    public static final String ATTRIBUTE_EXPOSE = "expose";
    public static final String ATTRIBUTE_CLASSIFY = "classify";
    public static final String ATTRIBUTE_MAIN_TABLE_COLUMNS = "mainTableColumns";
    public static final String ATTRIBUTE_REFERENCE_TABLE_COLUMNS = "referenceTableColumns";

    public static final String XML_TAG_HISTORY_FLAG = "ext:historyFlag";
    public static final String XML_TAG_TABLE_SUFFIX = "ext:tableSuffix";
    public static final String XML_TAG_COLUMNS = "ext:columns";
    public static final String XML_TAG_COLUMN = "ext:column";
    public static final String XML_TAG_SUBJECT_TABLE = "ext:subjectTable";
    public static final String XML_TAG_DCM_COLUMNS = "ext:dcmColumns";

    public static final String DISTRIBUTION_DISTRIBUTE_ALL = "distributeAll";
    public static final String DISTRIBUTION_DISTRIBUTE_PRIMARY = "distributePrimary";
    public static final String DISTRIBUTION_DISTRIBUTE_HISTORY = "distributeHistory";
    public static final String DISTRIBUTION_REFERENCE_ALL = "referenceAll";
    public static final String DISTRIBUTION_REFERENCE_PRIMARY = "referencePrimary";
    public static final String DISTRIBUTION_REFERENCE_HISTORY = "referenceHistory";

    public static final String SUFFIX_ID = "_id";
    public static final String SUFFIX_VIEW = "_v";
    public static final String SUFFIX_RELATION = "_rel";
    public static final String SUFFIX_M2M = "_m2m";

    public static final String PREFIX_INDEX = "ix_";
    public static final String PREFIX_UNIQUE_INDEX = "ui_";

    public static final String TYPE_TEXT = "text";
    public static final String TYPE_CHAR = "char";

    public static final String ROLE_PERMISSION_TABLE = "ddm_role_permission";
    public static final String ROLE_PERMISSION_ROLE_NAME = "role_name";
    public static final String ROLE_PERMISSION_OBJECT_NAME = "object_name";
    public static final String ROLE_PERMISSION_COLUMN_NAME = "column_name";
    public static final String ROLE_PERMISSION_OPERATION = "operation";

    public static final String METADATA_TABLE = "ddm_liquibase_metadata";
    public static final String METADATA_CHANGE_TYPE = "change_type";
    public static final String METADATA_CHANGE_NAME = "change_name";
    public static final String METADATA_ATTRIBUTE_NAME = "attribute_name";
    public static final String METADATA_ATTRIBUTE_VALUE = "attribute_value";

    public static final String SEARCH_METADATA_CHANGE_TYPE_VALUE = "searchCondition";
    public static final String SEARCH_METADATA_ATTRIBUTE_NAME_LIMIT = "limit";
    public static final String SEARCH_METADATA_ATTRIBUTE_NAME_PAGINATION = "pagination";

    public static final String TYPE_METADATA_CHANGE_TYPE_VALUE = "type";
    public static final String TYPE_METADATA_ATTRIBUTE_NAME_LABEL = "label";

    public static final String VERSIONING_METADATA_CHANGE_TYPE_VALUE = "versioning";
    public static final String VERSIONING_METADATA_CHANGE_NAME_VALUE = "registry_version";
    public static final String VERSIONING_METADATA_ATTRIBUTE_NAME_CURRENT = "current";

    public static final String OPERATOR_EQ = "eq";            // equal
    public static final String OPERATOR_NE = "ne";            // not equal
    public static final String OPERATOR_GT = "gt";            // greater than
    public static final String OPERATOR_GE = "ge";            // greater or equal
    public static final String OPERATOR_LT = "lt";            // less than
    public static final String OPERATOR_LE = "le";            // less or equal
    public static final String OPERATOR_IN = "in";            // in
    public static final String OPERATOR_NOT_IN = "notIn";     // not in
    public static final String OPERATOR_IS_NULL = "isNull";   // is null
    public static final String OPERATOR_SIMILAR = "similar";  // ~
    public static final String OPERATOR_LIKE = "like";        // like

    public static final String CONTEXT_PUB = "pub";
    public static final String CONTEXT_SUB = "sub";

    private DdmConstants() {}
}
