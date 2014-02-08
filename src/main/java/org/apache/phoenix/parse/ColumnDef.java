/*
 * Copyright 2014 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.SchemaUtil;


/**
 * 
 * Represents a column definition during DDL
 *
 * 
 * @since 0.1
 */
public class ColumnDef {
    private final ColumnName columnDefName;
    private final PDataType dataType;
    private final boolean isNull;
    private final Integer maxLength;
    private final Integer scale;
    private final boolean isPK;
    private final ColumnModifier columnModifier;
 
    ColumnDef(ColumnName columnDefName, String sqlTypeName, boolean isNull, Integer maxLength,
            Integer scale, boolean isPK, ColumnModifier columnModifier) {
        try {
            this.columnDefName = columnDefName;
            this.dataType = sqlTypeName == null ? null : PDataType.fromSqlTypeName(SchemaUtil.normalizeIdentifier(sqlTypeName));
            this.isNull = isNull;
            if (this.dataType == PDataType.CHAR) {
                if (maxLength == null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.MISSING_CHAR_LENGTH)
                        .setColumnName(columnDefName.getColumnName()).build().buildException();
                }
                if (maxLength < 1) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NONPOSITIVE_CHAR_LENGTH)
                        .setColumnName(columnDefName.getColumnName()).build().buildException();
                }
                scale = null;
            } else if (this.dataType == PDataType.VARCHAR) {
                if (maxLength != null && maxLength < 1) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NONPOSITIVE_CHAR_LENGTH)
                        .setColumnName(columnDefName.getColumnName()).build().buildException(); 
                }
                scale = null;
            } else if (this.dataType == PDataType.DECIMAL) {
            	Integer origMaxLength = maxLength;
                maxLength = maxLength == null ? PDataType.MAX_PRECISION : maxLength;
                // for deciaml, 1 <= maxLength <= PDataType.MAX_PRECISION;
                if (maxLength < 1 || maxLength > PDataType.MAX_PRECISION) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.DECIMAL_PRECISION_OUT_OF_RANGE)
                        .setColumnName(columnDefName.getColumnName()).build().buildException();
                }
                // When a precision is specified and a scale is not specified, it is set to 0. 
                // 
                // This is the standard as specified in
                // http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
                // and 
                // http://docs.oracle.com/javadb/10.6.2.1/ref/rrefsqlj15260.html.
                // Otherwise, if scale is bigger than maxLength, just set it to the maxLength;
                //
                // When neither a precision nor a scale is specified, the precision and scale is
                // ignored. All decimal are stored with as much decimal points as possible.
                scale = scale == null ? 
                		origMaxLength == null ? null : PDataType.DEFAULT_SCALE : 
                		scale > maxLength ? maxLength : scale; 
            } else if (this.dataType == PDataType.BINARY) {
                if (maxLength == null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.MISSING_BINARY_LENGTH)
                        .setColumnName(columnDefName.getColumnName()).build().buildException();
                }
                if (maxLength < 1) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NONPOSITIVE_BINARY_LENGTH)
                        .setColumnName(columnDefName.getColumnName()).build().buildException();
                }
                scale = null;
            } else if (this.dataType == PDataType.INTEGER) {
                maxLength = PDataType.INT_PRECISION;
                scale = PDataType.ZERO;
            } else if (this.dataType == PDataType.LONG) {
                maxLength = PDataType.LONG_PRECISION;
                scale = PDataType.ZERO;
            } else {
                // ignore maxLength and scale for other types.
                maxLength = null;
                scale = null;
            }
            this.maxLength = maxLength;
            this.scale = scale;
            this.isPK = isPK;
            this.columnModifier = columnModifier;
        } catch (SQLException e) {
            throw new ParseException(e);
        }
    }

    public ColumnName getColumnDefName() {
        return columnDefName;
    }

    public PDataType getDataType() {
        return dataType;
    }

    public boolean isNull() {
        return isNull;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public Integer getScale() {
        return scale;
    }

    public boolean isPK() {
        return isPK;
    }
    
    public ColumnModifier getColumnModifier() {
    	return columnModifier;
    }
}