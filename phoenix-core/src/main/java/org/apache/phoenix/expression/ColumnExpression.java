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
package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDatum;

/**
 * 
 * Common base class for column value accessors
 *
 * 
 * @since 0.1
 */
abstract public class ColumnExpression extends BaseTerminalExpression {
    protected PDataType type;
    private Integer byteSize;
    private boolean isNullable;
    private Integer maxLength;
    private Integer scale;
    private ColumnModifier columnModifier;

    public ColumnExpression() {
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (isNullable() ? 1231 : 1237);
        Integer maxLength = this.getByteSize();
        result = prime * result + ((maxLength == null) ? 0 : maxLength.hashCode());
        PDataType type = this.getDataType();
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnExpression other = (ColumnExpression)obj;
        if (this.isNullable() != other.isNullable()) return false;
        if (!Objects.equal(this.getByteSize(),other.getByteSize())) return false;
        if (this.getDataType() != other.getDataType()) return false;
        return true;
    }

    public ColumnExpression(PDatum datum) {
        this.type = datum.getDataType();
        this.isNullable = datum.isNullable();
        if (type.isFixedWidth() && type.getByteSize() == null) {
            this.byteSize = datum.getByteSize();
        }
        this.maxLength = datum.getMaxLength();
        this.scale = datum.getScale();
        this.columnModifier = datum.getColumnModifier();
    }

    @Override
    public boolean isNullable() {
       return isNullable;
    }
    
    @Override
    public PDataType getDataType() {
        return type;
    }
    
    @Override
    public ColumnModifier getColumnModifier() {
    	return columnModifier;
    }

    @Override
    public Integer getByteSize() {
        if (byteSize != null) {
            return byteSize;
        }
        return super.getByteSize();
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }

    @Override
    public Integer getScale() {
        return scale;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        // read/write type ordinal, maxLength presence, scale presence and isNullable bit together to save space
        int typeAndFlag = WritableUtils.readVInt(input);
        isNullable = (typeAndFlag & 0x01) != 0;
        if ((typeAndFlag & 0x02) != 0) {
            scale = WritableUtils.readVInt(input);
        }
        if ((typeAndFlag & 0x04) != 0) {
            maxLength = WritableUtils.readVInt(input);
        }
        type = PDataType.values()[typeAndFlag >>> 3];
        if (type.isFixedWidth() && type.getByteSize() == null) {
            byteSize = WritableUtils.readVInt(input);
        }
        columnModifier = ColumnModifier.fromSystemValue(WritableUtils.readVInt(input));
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // read/write type ordinal, maxLength presence, scale presence and isNullable bit together to save space
        int typeAndFlag = (isNullable ? 1 : 0) | ((scale != null ? 1 : 0) << 1) | ((maxLength != null ? 1 : 0) << 2)
                | (type.ordinal() << 3);
        WritableUtils.writeVInt(output,typeAndFlag);
        if (scale != null) {
            WritableUtils.writeVInt(output, scale);
        }
        if (maxLength != null) {
            WritableUtils.writeVInt(output, maxLength);
        }
        if (type.isFixedWidth() && type.getByteSize() == null) {
            WritableUtils.writeVInt(output, byteSize);
        }
        WritableUtils.writeVInt(output, ColumnModifier.toSystemValue(columnModifier));
    }
}
