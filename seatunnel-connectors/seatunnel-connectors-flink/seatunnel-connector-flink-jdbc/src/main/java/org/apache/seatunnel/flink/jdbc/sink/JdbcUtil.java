package org.apache.seatunnel.flink.jdbc.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum JdbcUtil {
    ;

    public static void setRecordToStatement(PreparedStatement upload, Map<String, Integer> fieldTypeMap, String[] variables, Row row) throws SQLException {
        if (variables == null || variables.length == 0) {
            return;
        }
        for (int index = 0; index < variables.length; index++) {
            String variable = variables[index];
            Object field = row.getField(variable);
            int jdbcType = fieldTypeMap.get(variable);
            JdbcUtils.setField(upload, jdbcType, field, index);
        }
    }

    public static int[] getSqlTypesArray(Map<String, Integer> fieldTypeMap, String[] variables) {
        int[] sqlTypes = new int[variables.length];
        for (int i = 0; i < variables.length; i++) {
            sqlTypes[i] = fieldTypeMap.get(variables[i]);
        }
        return sqlTypes;
    }

    public static Map<String, Integer> getFieldTypeMap(TableSchema schema, Map<String, TypeInformation> typeInformationMap) {
        Map<String, Integer> fieldTypeMap = new HashMap<>();
        for (String fieldName : schema.getFieldNames()) {
            TypeInformation<?> typeInformation = schema.getFieldType(fieldName).orElseThrow(() -> new RuntimeException("absent type information for field " + fieldName));
            typeInformationMap.put(fieldName, typeInformation);
            int jdbcType = JdbcTypeUtil.typeInformationToSqlType(typeInformation);
            fieldTypeMap.put(fieldName, jdbcType);
        }
        return fieldTypeMap;
    }

    public static Map<String, Integer> getFieldPositionMap(TableSchema schema) {
        Map<String, Integer> fieldTypeMap = new HashMap<>();
        int i = 0;
        for (String fieldName : schema.getFieldNames()) {
            fieldTypeMap.put(fieldName, i++);
        }
        return fieldTypeMap;
    }
}
