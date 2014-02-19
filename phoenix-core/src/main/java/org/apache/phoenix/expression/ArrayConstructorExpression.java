/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.schema.PArrayDataType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

/**
 * Creates an expression for Upsert with Values/Select using ARRAY
 */
public class ArrayConstructorExpression extends BaseCompoundExpression {
    private PDataType baseType;
    private int position = -1;
    private Object[] elements;
    private TrustedByteArrayOutputStream byteStream = null;
    private DataOutputStream oStream = null;
    private int estimatedSize = 0;
    private byte[] offsetArr;

    public ArrayConstructorExpression(List<Expression> children, PDataType baseType) {
        super(children);
        init(baseType);
        estimatedSize = PArrayDataType.estimateSize(this.children.size(), this.baseType);
        if (!this.baseType.isFixedWidth() || baseType.isCoercibleTo(PDataType.VARCHAR)) {
            offsetArr = new byte[estimatedSize];
            estimatedSize = estimatedSize + (2 * Bytes.SIZEOF_BYTE) + (2 * Bytes.SIZEOF_INT);
        } else {
            estimatedSize = estimatedSize + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT;
        }
        byteStream = new TrustedByteArrayOutputStream(estimatedSize);
        oStream = new DataOutputStream(byteStream);
    }

    private void init(PDataType baseType) {
        this.baseType = baseType;
        elements = new Object[getChildren().size()];
    }

    @Override
    public PDataType getDataType() {
        return PDataType.fromTypeId(baseType.getSqlType() + Types.ARRAY);
    }

    @Override
    public void reset() {
        super.reset();
        position = 0;
        Arrays.fill(elements, null);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        try {
            int offset = 0;
            // track the elementlength for variable array
            int elementLength = 0;
            for (int i = position >= 0 ? position : 0; i < elements.length; i++) {
                Expression child = children.get(i);
                if (!child.evaluate(tuple, ptr)) {
                    if (tuple != null && !tuple.isImmutable()) {
                        if (position >= 0) position = i;
                        return false;
                    }
                } else {
                    // track the offset position here from the size of the byteStream
                    if (!baseType.isFixedWidth() || baseType.isCoercibleTo(PDataType.VARCHAR)) {
                        Bytes.putInt(offsetArr, offset, (byteStream.size()));
                        offset += Bytes.SIZEOF_INT;
                        elementLength += ptr.getLength();
                    }
                    if (!child.isStateless()) {
                        oStream.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                    } else {
                        oStream.write(ptr.get());
                    }
                }
            }
            if (position >= 0) position = elements.length;
            if (!baseType.isFixedWidth() || baseType.isCoercibleTo(PDataType.VARCHAR)) {
                oStream.writeByte(0);
                int offsetPosition = (byteStream.size());
                oStream.write(offsetArr);
                oStream.writeInt(offsetPosition);
            }
            // No of elements - writing it as negative as we treat the array not to fit in Short size
            oStream.writeInt(-(children.size()));
            // Version of the array
            oStream.write(PArrayDataType.ARRAY_SERIALIZATION_VERSION);
            // For variable length arrays setting this way would mean that we discard the additional 0 bytes in the
            // array
            // that gets created because we don't know the exact size. For fixed length array elementLength = 0
            ptr.set(byteStream.getBuffer(), 0, estimatedSize + elementLength);
            return true;
        } catch (IOException e) {
            throw new RuntimeException("Exception while serializing the byte array");
        } finally {
            try {
                byteStream.close();
                oStream.close();
            } catch (IOException e) {
                // Should not happen
            }
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        int baseTypeOrdinal = WritableUtils.readVInt(input);
        init(PDataType.values()[baseTypeOrdinal]);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, baseType.ordinal());
    }

}