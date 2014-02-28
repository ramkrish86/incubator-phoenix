/*
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
package org.apache.phoenix.schema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Arrays;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

/**
 * The datatype for PColummns that are Arrays. Any variable length array would follow the below order. 
 * Every element would be seperated by a seperator byte '0'. Null elements are counted and once a first 
 * non null element appears we write the count of the nulls prefixed with a seperator byte.
 * Trailing nulls are not taken into account. The last non null element is followed by two seperator bytes. 
 * For eg a, b, null, null, c, null -> 65 0 66 0 0 2 67 0 0 0 
 * a null null null b c null d -> 65 0 0 3 66 0 67 0 0 1 68 0 0 0.
 * The reason we use this serialization format is to allow the
 * byte array of arrays of the same type to be directly comparable against each other. 
 * This prevents a costly deserialization on compare and allows an array column to be used as the last column in a primary key constraint.
 */
public class PArrayDataType {

    public static final byte ARRAY_SERIALIZATION_VERSION = 1;
	public PArrayDataType() {
	}

	public byte[] toBytes(Object object, PDataType baseType) {
		if(object == null) {
			throw new ConstraintViolationException(this + " may not be null");
		}
		Pair<Integer, Integer> nullsVsNullRepeationCounter = new Pair<Integer, Integer>();
        int size = estimateByteSize(object, nullsVsNullRepeationCounter,
                PDataType.fromTypeId((baseType.getSqlType() + Types.ARRAY)));
        int noOfElements = ((PhoenixArray)object).numElements;
        if(noOfElements == 0) {
        	return ByteUtil.EMPTY_BYTE_ARRAY;
        }
        
        TrustedByteArrayOutputStream byteStream = null;
		if (!baseType.isFixedWidth()) {
		    size += ((2 * Bytes.SIZEOF_BYTE) + (noOfElements - nullsVsNullRepeationCounter.getFirst()) * Bytes.SIZEOF_BYTE)
		                                + (nullsVsNullRepeationCounter.getSecond() * 2 * Bytes.SIZEOF_BYTE);
		    // Assume an offset array that fit into Short.MAX_VALUE
		    int capacity = noOfElements * Bytes.SIZEOF_SHORT;
		    // Here the int for noofelements, byte for the version, int for the offsetarray position and 2 bytes for the end seperator
            byteStream = new TrustedByteArrayOutputStream(size + capacity + Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE +  Bytes.SIZEOF_INT);
		} else {
		    // Here the int for noofelements, byte for the version
		    byteStream = new TrustedByteArrayOutputStream(size + Bytes.SIZEOF_INT+ Bytes.SIZEOF_BYTE);
		}
		DataOutputStream oStream = new DataOutputStream(byteStream);
		return createArrayBytes(byteStream, oStream, (PhoenixArray)object, noOfElements, baseType);
	}

    public static int serializeNulls(DataOutputStream oStream, int nulls) throws IOException {
        if (nulls > 0) {
            oStream.write(QueryConstants.SEPARATOR_BYTE);
            int nNullsWritten = nulls;
            do {
                byte nNullsWrittenInBytes = (byte)((nulls % 256));
                nNullsWrittenInBytes = (byte)(nNullsWrittenInBytes & 0xf);
                oStream.write(SortOrder.invert(nNullsWrittenInBytes)); // Single byte for repeating nulls
                nulls -= nNullsWritten;
            } while (nulls > 0);
        }
        return nulls;
    }
 
    public static void writeEndSeperatorForVarLengthArray(DataOutputStream oStream) throws IOException {
        oStream.write(QueryConstants.SEPARATOR_BYTE);
        oStream.write(QueryConstants.SEPARATOR_BYTE);
    }

	public static boolean useShortForOffsetArray(int maxOffset) {
		// If the max offset is less than Short.MAX_VALUE then offset array can use short
		if (maxOffset <= (2 * Short.MAX_VALUE)) {
			return true;
		}
		// else offset array can use Int
		return false;
	}

	public int toBytes(Object object, byte[] bytes, int offset) {
	    PhoenixArray array = (PhoenixArray)object;
        if (array == null || array.baseType == null) {
            return 0;
        }
        return estimateByteSize(object, null, PDataType.fromTypeId((array.baseType.getSqlType() + Types.ARRAY)));
	}

	// Estimates the size of the given array and also calculates the number of nulls and its repetition factor
    public int estimateByteSize(Object o, Pair<Integer, Integer> nullsVsNullRepeationCounter, PDataType baseType) {
        if (baseType.isFixedWidth()) { return baseType.getByteSize(); }
        if (baseType.isArrayType()) {
            PhoenixArray array = (PhoenixArray)o;
            int noOfElements = array.numElements;
            int totalVarSize = 0;
            int nullsRepeationCounter = 0;
            int nulls = 0;
            int totalNulls = 0;
            for (int i = 0; i < noOfElements; i++) {
                if (baseType == PDataType.CHAR_ARRAY) {
                    totalVarSize += array.getMaxLength();
                } else {
                    totalVarSize += array.estimateByteSize(i);
                    if (!PDataType.fromTypeId((baseType.getSqlType() - Types.ARRAY)).isFixedWidth()) {
                        if (array.isNull(i)) {
                            nulls++;
                        } else {
                            if (nulls > 0) {
                                totalNulls += nulls;
                                nulls = 0;
                                nullsRepeationCounter++;
                            }
                        }
                    }
                }
            }
            if (nullsVsNullRepeationCounter != null) {
                if (nulls > 0) {
                    totalNulls += nulls;
                    // do not increment nullsRepeationCounter to identify trailing nulls
                }
                nullsVsNullRepeationCounter.setFirst(totalNulls);
                nullsVsNullRepeationCounter.setSecond(nullsRepeationCounter);
            }
            return totalVarSize;
        }
        // Non fixed width types must override this
        throw new UnsupportedOperationException();
    }
    
	public boolean isCoercibleTo(PDataType targetType, Object value) {
	    return targetType.isCoercibleTo(targetType, value);
	}
	
	public boolean isCoercibleTo(PDataType targetType, PDataType expectedTargetType) {
		if(!targetType.isArrayType()) {
			return false;
		} else {
			PDataType targetElementType = PDataType.fromTypeId(targetType.getSqlType()
					- Types.ARRAY);
			PDataType expectedTargetElementType = PDataType.fromTypeId(expectedTargetType
					.getSqlType() - Types.ARRAY);
			return expectedTargetElementType.isCoercibleTo(targetElementType);
		}
    }
	
	public boolean isSizeCompatible(PDataType srcType, Object value,
			byte[] b, Integer maxLength, Integer desiredMaxLength,
			Integer scale, Integer desiredScale) {
		PhoenixArray pArr = (PhoenixArray) value;
		Object[] charArr = (Object[]) pArr.array;
		PDataType baseType = PDataType.fromTypeId(srcType.getSqlType()
				- Types.ARRAY);
		for (int i = 0 ; i < charArr.length; i++) {
			if (!baseType.isSizeCompatible(baseType, value, b, maxLength,
					desiredMaxLength, scale, desiredScale)) {
				return false;
			}
		}
		return true;
	}


    public Object toObject(String value) {
		// TODO: Do this as done in CSVLoader
		throw new IllegalArgumentException("This operation is not suppported");
	}

	public Object toObject(byte[] bytes, int offset, int length, PDataType baseType, 
			SortOrder sortOrder) {
		return createPhoenixArray(bytes, offset, length, sortOrder,
				baseType);
	}

    public static void positionAtArrayElement(ImmutableBytesWritable ptr, int arrayIndex, PDataType baseDataType,
            Integer byteSize) {
        byte[] bytes = ptr.get();
        int initPos = ptr.getOffset();
        int noOfElements = 0;
        noOfElements = Bytes.toInt(bytes, (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)),
                Bytes.SIZEOF_INT);

        if (arrayIndex >= noOfElements) { throw new IndexOutOfBoundsException("Invalid index " + arrayIndex
                + " specified, greater than the no of eloements in the array: " + noOfElements); }
        boolean useShort = true;
        if (noOfElements < 0) {
            noOfElements = -noOfElements;
            useShort = false;
        }

        if (!baseDataType.isFixedWidth()) {
            int indexOffset = Bytes.toInt(bytes,
                    (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + 2 * Bytes.SIZEOF_INT))) + ptr.getOffset();
            if(arrayIndex >= noOfElements) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            } else {
                // Skip those many offsets as given in the arrayIndex
                // If suppose there are 5 elements in the array and the arrayIndex = 3
                // This means we need to read the 4th element of the array
                // So inorder to know the length of the 4th element we will read the offset of 4th element and the
                // offset of 5th element.
                // Subtracting the offset of 5th element and 4th element will give the length of 4th element
                // So we could just skip reading the other elements.
                int currOffset = getOffset(bytes, arrayIndex, useShort, indexOffset);
                int elementLength = 0;
                if (arrayIndex == (noOfElements - 1)) {
                    elementLength = bytes[currOffset + initPos] == QueryConstants.SEPARATOR_BYTE ? 0 : indexOffset
                            - currOffset - 3;
                } else {
                    elementLength = bytes[currOffset + initPos] == QueryConstants.SEPARATOR_BYTE ? 0 : getOffset(bytes,
                            arrayIndex + 1, useShort, indexOffset) - currOffset - 1;
                }
                ptr.set(bytes, currOffset + initPos, elementLength);
            }
        } else {
            if (byteSize != null) {
                ptr.set(bytes, ptr.getOffset() + (arrayIndex * byteSize), byteSize);
            } else {
                ptr.set(bytes, ptr.getOffset() + (arrayIndex * baseDataType.getByteSize()), baseDataType.getByteSize());
            }
        }
    }

    private static int getOffset(byte[] bytes, int arrayIndex, boolean useShort, int indexOffset) {
        int offset;
        if (useShort) {
            offset = indexOffset + (Bytes.SIZEOF_SHORT * arrayIndex);
            return Bytes.toShort(bytes, offset, Bytes.SIZEOF_SHORT) + Short.MAX_VALUE;
        } else {
            offset = indexOffset + (Bytes.SIZEOF_INT * arrayIndex);
            return Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
        }
    }
    
    private static int getOffset(ByteBuffer indexBuffer, int arrayIndex, boolean useShort, int indexOffset ) {
        int offset;
        if(useShort) {
            offset = indexBuffer.getShort() + Short.MAX_VALUE;
        } else {
            offset = indexBuffer.getInt();
        }
        return offset;
    }

	public Object toObject(byte[] bytes, int offset, int length, PDataType baseType) {
		return toObject(bytes, offset, length, baseType, SortOrder.getDefault());
	}
	
	public Object toObject(Object object, PDataType actualType) {
		return object;
	}

	public Object toObject(Object object, PDataType actualType, SortOrder sortOrder) {
		// How to use the sortOrder ? Just reverse the elements
		return toObject(object, actualType);
	}
	
	/**
	 * ser
	 * @param byteStream
	 * @param oStream
	 * @param array
	 * @param noOfElements
	 * @param baseType
	 * @param capacity
	 * @return
	 */
    private byte[] createArrayBytes(TrustedByteArrayOutputStream byteStream, DataOutputStream oStream,
            PhoenixArray array, int noOfElements, PDataType baseType) {
        try {
            if (!baseType.isFixedWidth()) {
                int[] offsetPos = new int[noOfElements];
                int nulls = 0;
                for (int i = 0; i < noOfElements; i++) {
                    byte[] bytes = array.toBytes(i);
                    if (bytes.length == 0) {
                        offsetPos[i] = byteStream.size();
                        nulls++;
                    } else {
                        nulls = serializeNulls(oStream, nulls);
                        offsetPos[i] = byteStream.size();
                        oStream.write(bytes, 0, bytes.length);
                        oStream.write(QueryConstants.SEPARATOR_BYTE);
                    }
                }
                // Double seperator byte to show end of the non null array
                PArrayDataType.writeEndSeperatorForVarLengthArray(oStream);
                noOfElements = PArrayDataType.serailizeOffsetArrayIntoStream(oStream, byteStream, noOfElements,
                        offsetPos[offsetPos.length - 1], offsetPos);
            } else {
                for (int i = 0; i < noOfElements; i++) {
                    byte[] bytes = array.toBytes(i);
                    int length = bytes.length;
                    if(baseType == PDataType.CHAR) {
                        byte[] bytesWithPad = new byte[array.getMaxLength()];
                        Arrays.fill(bytesWithPad, StringUtil.SPACE_UTF8);
                        System.arraycopy(bytes, 0, bytesWithPad, 0, length);
                        oStream.write(bytesWithPad, 0, bytesWithPad.length);
                    } else {
                        oStream.write(bytes, 0, length);
                    }
                }
            }
            serializeHeaderInfoIntoStream(oStream, noOfElements);
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            ptr.set(byteStream.getBuffer(), 0, byteStream.size());
            return ByteUtil.copyKeyBytesIfNecessary(ptr);
        } catch (IOException e) {
            try {
                byteStream.close();
                oStream.close();
            } catch (IOException ioe) {

            }
        }
        // This should not happen
        return null;
    }

    public static int serailizeOffsetArrayIntoStream(DataOutputStream oStream, TrustedByteArrayOutputStream byteStream,
            int noOfElements, int maxOffset, int[] offsetPos) throws IOException {
        int offsetPosition = (byteStream.size());
        byte[] offsetArr = null;
        boolean useInt = true;
        if (PArrayDataType.useShortForOffsetArray(maxOffset)) {
            offsetArr = new byte[PArrayDataType.initOffsetArray(noOfElements, Bytes.SIZEOF_SHORT)];
            useInt = false;
        } else {
            offsetArr = new byte[PArrayDataType.initOffsetArray(noOfElements, Bytes.SIZEOF_INT)];
            noOfElements = -noOfElements;
        }
        int off = 0;
        if(useInt) {
            for (int pos : offsetPos) {
                Bytes.putInt(offsetArr, off, pos);
                off += Bytes.SIZEOF_INT;
            }
        } else {
            for (int pos : offsetPos) {
                Bytes.putShort(offsetArr, off, (short)(pos - Short.MAX_VALUE));
                off += Bytes.SIZEOF_SHORT;
            }
        }
        oStream.write(offsetArr);
        oStream.writeInt(offsetPosition);
        return noOfElements;
    }

    public static void serializeHeaderInfoIntoBuffer(ByteBuffer buffer, int noOfElements) {
        // No of elements
        buffer.putInt(noOfElements);
        // Version of the array
        buffer.put(ARRAY_SERIALIZATION_VERSION);
    }

    public static void serializeHeaderInfoIntoStream(DataOutputStream oStream, int noOfElements) throws IOException {
        // No of elements
        oStream.writeInt(noOfElements);
        // Version of the array
        oStream.write(ARRAY_SERIALIZATION_VERSION);
    }

	public static int initOffsetArray(int noOfElements, int baseSize) {
		// for now create an offset array equal to the noofelements
		return noOfElements * baseSize;
    }

    // Any variable length array would follow the below order
    // Every element would be seperated by a seperator byte '0'
    // Null elements are counted and once a first non null element appears we
    // write the count of the nulls prefixed with a seperator byte
    // Trailing nulls are not taken into account
    // The last non null element is followed by two seperator bytes
    // For eg
    // a, b, null, null, c, null would be 
    // 65 0 66 0 0 2 67 0 0 0
    // a null null null b c null d would be
    // 65 0 0 3 66 0 67 0 0 1 68 0 0 0
	// Follow the above example to understand how this works
    private Object createPhoenixArray(byte[] bytes, int offset, int length, SortOrder sortOrder, PDataType baseDataType) {
        if (bytes == null || bytes.length == 0) { return null; }
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
        int initPos = buffer.position();
        buffer.position((buffer.limit() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)));
        int noOfElemPos = buffer.position();
        int noOfElements = buffer.getInt();
        boolean useShort = true;
        int baseSize = Bytes.SIZEOF_SHORT;
        if (noOfElements < 0) {
            noOfElements = -noOfElements;
            baseSize = Bytes.SIZEOF_INT;
            useShort = false;
        }
        Object[] elements = (Object[])java.lang.reflect.Array.newInstance(baseDataType.getJavaClass(), noOfElements);
        if (!baseDataType.isFixedWidth()) {
            buffer.position(buffer.limit() - (Bytes.SIZEOF_BYTE + (2 * Bytes.SIZEOF_INT)));
            int indexOffset = buffer.getInt();
            buffer.position(initPos);
            buffer.position(indexOffset + initPos);
            ByteBuffer indexArr = ByteBuffer.allocate(initOffsetArray(noOfElements, baseSize));
            byte[] array = indexArr.array();
            buffer.get(array);
            int countOfElementsRead = 0;
            int i = 0;
            int currOffset = -1;
            int nextOff = -1;
            boolean foundNull = false;
            if (noOfElements != 0) {
                while (countOfElementsRead <= noOfElements) {
                    if (countOfElementsRead == 0) {
                        currOffset = getOffset(indexArr, countOfElementsRead, useShort, indexOffset);
                        countOfElementsRead++;
                    } else {
                        currOffset = nextOff;
                    }
                    if (countOfElementsRead == noOfElements) {
                        nextOff = indexOffset - 2;
                    } else {
                        nextOff = getOffset(indexArr, countOfElementsRead + 1, useShort, indexOffset);
                    }
                    countOfElementsRead++;
                    if ((bytes[currOffset + initPos] != QueryConstants.SEPARATOR_BYTE) && foundNull) {
                        // Found a non null element
                        foundNull = false;
                    }
                    if (bytes[currOffset + initPos] == QueryConstants.SEPARATOR_BYTE) {
                        // Null element
                        foundNull = true;
                        i++;
                        continue;
                    }
                    int elementLength = nextOff - currOffset;
                    buffer.position(currOffset + initPos);
                    // Subtract the seperator from the element length
                    byte[] val = new byte[elementLength - 1];
                    buffer.get(val);
                    elements[i++] = baseDataType.toObject(val, sortOrder);
                }
            }
        } else {
            buffer.position(initPos);
            for (int i = 0; i < noOfElements; i++) {
                byte[] val;
                if (baseDataType == PDataType.CHAR) {
                    int maxLength = (noOfElemPos - initPos)/noOfElements;
                    // Should be char array
                    val = new byte[maxLength];
                } else {
                    val = new byte[baseDataType.getByteSize()];
                }
                buffer.get(val);
                elements[i] = baseDataType.toObject(val, sortOrder);
            }
        }
        return PArrayDataType.instantiatePhoenixArray(baseDataType, elements);
    }
	
    public static PhoenixArray instantiatePhoenixArray(PDataType actualType, Object[] elements) {
        return PDataType.instantiatePhoenixArray(actualType, elements);
    }
	
	public int compareTo(Object lhs, Object rhs) {
		PhoenixArray lhsArr = (PhoenixArray) lhs;
		PhoenixArray rhsArr = (PhoenixArray) rhs;
		if(lhsArr.equals(rhsArr)) {
			return 0;
		}
		return 1;
	}

	public static int getArrayLength(ImmutableBytesWritable ptr,
			PDataType baseType) {
		byte[] bytes = ptr.get();
		if(baseType.isFixedWidth()) {
			return ((ptr.getLength() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT))/baseType.getByteSize());
		}
		return Bytes.toInt(bytes, (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)));
	}

    public static int estimateSize(int size, PDataType baseType) {
        if(baseType.isFixedWidth()) {
            return baseType.getByteSize() * size;
        } else {
            return size * ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
        }
        
    }

}