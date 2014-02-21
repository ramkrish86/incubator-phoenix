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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

/**
 * The datatype for PColummns that are Arrays
 */
public class PArrayDataType {

    public static final byte ARRAY_SERIALIZATION_VERSION = 1;
	public PArrayDataType() {
	}

	public byte[] toBytes(Object object, PDataType baseType) {
		if(object == null) {
			throw new ConstraintViolationException(this + " may not be null");
		}
		Pair<Integer, Pair<Integer,Integer>> nullsVsNullRepeationCounter = new Pair<Integer, Pair<Integer, Integer>>();
        int size = estimateByteSize(object, nullsVsNullRepeationCounter,
                PDataType.fromTypeId((baseType.getSqlType() + Types.ARRAY)));
        int noOfElements = ((PhoenixArray)object).numElements;
        if(noOfElements == 0) {
        	return ByteUtil.EMPTY_BYTE_ARRAY;
        }
        int trailingNulls = nullsVsNullRepeationCounter.getSecond().getSecond();
        ByteBuffer buffer;
        int capacity = 0;
		if (!baseType.isFixedWidth() || baseType.isCoercibleTo(PDataType.VARCHAR)) {
			// variable
			if (useShortForOffsetArray(size)) {
				// Use Short to represent the offset
				capacity = initOffsetArray(noOfElements - trailingNulls, Bytes.SIZEOF_SHORT);
			} else {
				capacity = initOffsetArray(noOfElements - trailingNulls, Bytes.SIZEOF_INT);
				// Negate the number of elements
				noOfElements = -noOfElements;
			}
			// 2 bytes for the end double seperator + non null value byte separtor + 2 bytes for denoting the count of
			// successive null bytes prefixed with a seperator byte
			// So if there is an array
			// a b c null null d - 65 0 66 0 67 0 0 2 68 0 0 0

            size += ((2 * Bytes.SIZEOF_BYTE) + (noOfElements - nullsVsNullRepeationCounter.getFirst()) * Bytes.SIZEOF_BYTE)
                    + (nullsVsNullRepeationCounter.getSecond().getFirst() * 2 * Bytes.SIZEOF_BYTE);

			// Here the int for noofelements, byte for the version, int for the offsetarray position, trailing nulls
            buffer = ByteBuffer.allocate(size + capacity + Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE + 2 * Bytes.SIZEOF_INT );
		} else {
		    // Here the int for noofelements, byte for the version, trailing nulls
			buffer = ByteBuffer.allocate(size + Bytes.SIZEOF_INT+ Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT);
		}
		return bytesFromByteBuffer((PhoenixArray)object, buffer, noOfElements, baseType, capacity, trailingNulls);
	}

    public static int serializeNulls(DataOutputStream oStream, int nulls) throws IOException {
        if (nulls > 0) {
            oStream.write(QueryConstants.SEPARATOR_BYTE);
            do {
                byte nNullsWritten = (byte)(nulls % 256);
                oStream.write(nNullsWritten); // Single byte for repeating nulls
                nulls -= nNullsWritten;
            } while (nulls > 0);
        }
        return nulls;
    }
    
    private static int serializeNullsIntoBuffer(ByteBuffer buffer, int nulls) {
        if (nulls > 0) {
            buffer.put(QueryConstants.SEPARATOR_BYTE);
            do {
                byte nNullsWritten = (byte)(nulls % 256);
                buffer.put(nNullsWritten); // Single byte for repeating nulls
                nulls -= nNullsWritten;
            } while (nulls > 0);
        }
        return nulls;
    }
 
    public static void writeEndSeperatorForVarLengthArray(DataOutputStream oStream) throws IOException {
        oStream.write(QueryConstants.SEPARATOR_BYTE);
        oStream.write(QueryConstants.SEPARATOR_BYTE);
    }

	public static boolean useShortForOffsetArray(int size) {
		// If the total size is less than Short.MAX_VALUE then offset array can use short
		if (size <= (2 * Short.MAX_VALUE)) {
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

    public int estimateByteSize(Object o, Pair<Integer, Pair<Integer,Integer>> nullsVsNullRepeationCounter, PDataType baseType) {
        if (baseType.isFixedWidth()) { return baseType.getByteSize(); }
        if (baseType.isArrayType()) {
            PhoenixArray array = (PhoenixArray)o;
            int noOfElements = array.numElements;
            int totalVarSize = 0;
            int nullsRepeationCounter = 0;
            int nulls = 0;
            int totalNulls = 0;
            int trailingNulls = 0;
            for (int i = 0; i < noOfElements; i++) {
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
            if (nullsVsNullRepeationCounter != null) {
                if (nulls > 0) {
                    totalNulls += nulls;
                    trailingNulls = nulls;
                    // do not increment nullsRepeationCounter to identify trailing nulls
                }
                nullsVsNullRepeationCounter.setFirst(totalNulls);
                Pair<Integer, Integer> innerPair = new Pair<Integer, Integer>();
                innerPair.setFirst(nullsRepeationCounter);
                innerPair.setSecond(trailingNulls);
                nullsVsNullRepeationCounter.setSecond(innerPair);
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
	
	public static void positionAtArrayElement(ImmutableBytesWritable ptr, int arrayIndex, PDataType baseDataType) {
		byte[] bytes = ptr.get();
		int initPos = ptr.getOffset();
		int noOfElements = 0;
		noOfElements = Bytes.toInt(bytes, (ptr.getOffset() + ptr.getLength() - ( Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)), Bytes.SIZEOF_INT);
		int trailingNulls = 0;
		trailingNulls = Bytes.toInt(bytes, (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + 2 * Bytes.SIZEOF_INT)), Bytes.SIZEOF_INT);
		
		if(arrayIndex >= noOfElements) {
			throw new IndexOutOfBoundsException(
					"Invalid index "
							+ arrayIndex
							+ " specified, greater than the no of eloements in the array: "
							+ noOfElements);
		}
		if(arrayIndex >= (noOfElements - trailingNulls)) {
		    ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
		    return;
		}
		boolean useShort = true;
		int baseSize = Bytes.SIZEOF_SHORT;
		if (noOfElements < 0) {
			noOfElements = -noOfElements;
			baseSize = Bytes.SIZEOF_INT;
			useShort = false;
		}

		if (baseDataType.getByteSize() == null) {
		    int offset = ptr.getOffset();
            int indexOffset = Bytes.toInt(bytes,
                    (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + 3 * Bytes.SIZEOF_INT))) + ptr.getOffset();
		    int valArrayPostion = offset;
			int currOff = 0;
			if (noOfElements - trailingNulls > 1) {
				while (offset <= (initPos+ptr.getLength())) {
					int nextOff = 0;
					// Skip those many offsets as given in the arrayIndex
					// If suppose there are 5 elements in the array and the arrayIndex = 3
					// This means we need to read the 4th element of the array
					// So inorder to know the length of the 4th element we will read the offset of 4th element and the offset of 5th element.
					// Subtracting the offset of 5th element and 4th element will give the length of 4th element
					// So we could just skip reading the other elements.
					if(useShort) {
						// If the arrayIndex is already the last element then read the last before one element and the last element
						offset = indexOffset + (Bytes.SIZEOF_SHORT * arrayIndex);
						if (arrayIndex == (noOfElements - 1)) {
							currOff = Bytes.toShort(bytes, offset, baseSize) + Short.MAX_VALUE;
							nextOff = indexOffset;
							offset += baseSize;
						} else {
							currOff = Bytes.toShort(bytes, offset, baseSize) + Short.MAX_VALUE;
							offset += baseSize;
							nextOff = Bytes.toShort(bytes, offset, baseSize) + Short.MAX_VALUE;
							offset += baseSize;
						}
					} else {
						// If the arrayIndex is already the last element then read the last before one element and the last element
						offset = indexOffset + (Bytes.SIZEOF_INT * arrayIndex);
						if (arrayIndex == (noOfElements - 1)) {
							currOff = Bytes.toInt(bytes, offset, baseSize);
							nextOff = indexOffset;
							offset += baseSize;
						} else {
							currOff = Bytes.toInt(bytes, offset, baseSize);
							offset += baseSize;
							nextOff = Bytes.toInt(bytes, offset, baseSize);
							offset += baseSize;
						}
					}
					int elementLength = nextOff - currOff;
					if(elementLength == 0) {
					    // Means a null element
					    ptr.set(bytes, currOff + initPos, elementLength);
					}
					if(currOff + initPos + elementLength == indexOffset) {
					    // Subtract 3 bytes - 1 for the seperator for the element and the 2 sepeator bytes at the end
					    ptr.set(bytes, currOff + initPos, elementLength - 3);
					} else {
					    // In case of odd number of elements the end seperator would not be there
					    ptr.set(bytes, currOff + initPos, elementLength - 1);
					}
					break;
				}
			} else {
			    // Subtract 3 bytes - 1 for the seperator for the element and the 2 sepeator bytes at the end
                ptr.set(bytes, valArrayPostion + initPos, (indexOffset - 3  - valArrayPostion));
			}
		} else {
			ptr.set(bytes,
					ptr.getOffset() + arrayIndex * baseDataType.getByteSize()
							, baseDataType.getByteSize());
		}
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
	
	// Making this private
	/**
	 * The format of the byte buffer looks like this for variable length array elements
	 * <noofelements><Offset of the index array><elements><offset array>
	 * where <noOfelements> - vint
	 * <offset of the index array> - int
	 * <elements>  - these are the values
	 * <offset array> - offset of every element written as INT/SHORT
	 * 
	 * @param array
	 * @param buffer
	 * @param noOfElements
	 * @param byteSize
	 * @param capacity 
	 * @param trailingNulls 
	 * @return
	 */
	private byte[] bytesFromByteBuffer(PhoenixArray array, ByteBuffer buffer,
			int noOfElements, PDataType baseType, int capacity, int trailingNulls) {
        if (buffer == null) return null;
        if (!baseType.isFixedWidth() || baseType.isCoercibleTo(PDataType.VARCHAR)) {
            int tempNoOfElements = noOfElements;
            ByteBuffer offsetArray = ByteBuffer.allocate(capacity);
            if(tempNoOfElements < 0){
                tempNoOfElements = -tempNoOfElements;
            }
            int nulls = 0;
            for (int i = 0; i < tempNoOfElements - trailingNulls; i++) {
                byte[] bytes = array.toBytes(i);
                markOffset(buffer, noOfElements, offsetArray);
                if(bytes.length == 0){
                   nulls++;
                } else {
                    nulls = serializeNullsIntoBuffer(buffer, nulls);
                    buffer.put(bytes);
                    buffer.put(QueryConstants.SEPARATOR_BYTE);
                }
            }
            if(nulls > 0) {
                nulls = serializeNullsIntoBuffer(buffer, nulls);
            }
            buffer.put(QueryConstants.SEPARATOR_BYTE);
            buffer.put(QueryConstants.SEPARATOR_BYTE);
            int offsetArrayPosition = buffer.position();
            buffer.put(offsetArray.array());
            buffer.putInt(offsetArrayPosition);
        } else {
            for (int i = 0; i < noOfElements; i++) {
                byte[] bytes = array.toBytes(i);
                buffer.put(bytes);
            }
        }
        serializeHeaderInfoIntoBuffer(buffer, noOfElements, trailingNulls);
        return buffer.array();
	}

    private void markOffset(ByteBuffer buffer, int noOfElements, ByteBuffer offsetArray) {
        if (noOfElements < 0) {
            offsetArray.putInt(buffer.position());
        } else {
            offsetArray.putShort((short)(buffer.position() - Short.MAX_VALUE));
        }
    }

    public static int serailizeOffsetArrayIntoStream(DataOutputStream oStream, TrustedByteArrayOutputStream byteStream,
            int noOfElements, int elementLength, int[] offsetPos) throws IOException {
        int offsetPosition = (byteStream.size());
        byte[] offsetArr = null;
        int incr = 0;
        boolean useInt = true;
        if (PArrayDataType.useShortForOffsetArray(elementLength)) {
            offsetArr = new byte[PArrayDataType.initOffsetArray(noOfElements, Bytes.SIZEOF_SHORT)];
            incr = Bytes.SIZEOF_SHORT;
            useInt = false;
        } else {
            offsetArr = new byte[PArrayDataType.initOffsetArray(noOfElements, Bytes.SIZEOF_INT)];
            incr = Bytes.SIZEOF_INT;
            noOfElements = -noOfElements;
        }
        int off = 0;
        off = fillOffsetArr(offsetPos, offsetArr, incr, off, useInt);
        oStream.write(offsetArr);
        oStream.writeInt(offsetPosition);
        return noOfElements;
    }

    private static int fillOffsetArr(int[] offsetPos, byte[] offsetArr, int incr, int off, boolean useInt) {
        for (int pos : offsetPos) {
            if (useInt) {
                Bytes.putInt(offsetArr, off, pos);
            } else {
                Bytes.putShort(offsetArr, off, (short)(pos - Short.MAX_VALUE));
            }
            off += incr;
        }
        return off;
    }

    public static void serializeHeaderInfoIntoBuffer(ByteBuffer buffer, int noOfElements, int trailingNulls) {
        // No of trailing nulls
        buffer.putInt(trailingNulls);
        // No of elements
        buffer.putInt(noOfElements);
        // Version of the array
        buffer.put(ARRAY_SERIALIZATION_VERSION);
    }

    public static void serializeHeaderInfoIntoStream(DataOutputStream oStream, int noOfElements) throws IOException {
        // No of trailing nulls
        // TODO
        oStream.writeInt(0);
        // No of elements
        oStream.writeInt(noOfElements);
        // Version of the array
        oStream.write(ARRAY_SERIALIZATION_VERSION);
    }

	public static int initOffsetArray(int noOfElements, int baseSize) {
		// for now create an offset array equal to the noofelements
		return noOfElements * baseSize;
    }

	private Object createPhoenixArray(byte[] bytes, int offset, int length,
			SortOrder sortOrder, PDataType baseDataType) {
		if(bytes == null || bytes.length == 0) {
			return null;
		}
		ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
		int initPos = buffer.position();
		buffer.position((buffer.limit() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)));
		int noOfElements = buffer.getInt();
        buffer.position((buffer.limit() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT + Bytes.SIZEOF_INT)));
		int trailingNulls = buffer.getInt();
		boolean useShort = true;
		int baseSize = Bytes.SIZEOF_SHORT;
		if(noOfElements < 0) {
			noOfElements = -noOfElements;
			baseSize = Bytes.SIZEOF_INT;
			useShort = false;
		}
		Object[] elements = (Object[]) java.lang.reflect.Array.newInstance(
				baseDataType.getJavaClass(), noOfElements);
        if (!baseDataType.isFixedWidth() || baseDataType.isCoercibleTo(PDataType.VARCHAR)) {
            buffer.position(buffer.limit() - (Bytes.SIZEOF_BYTE + (3 * Bytes.SIZEOF_INT)));
            int indexOffset = buffer.getInt();
            buffer.position(initPos);
            int valArrayPostion = buffer.position();
            buffer.position(indexOffset + initPos);
            ByteBuffer indexArr = ByteBuffer.allocate(initOffsetArray(noOfElements - trailingNulls, baseSize));
            byte[] array = indexArr.array();
            buffer.get(array);
            int countOfElementsRead = 0;
            int i = 0;
            int currOff = -1;
            int nextOff = -1;
            boolean foundNull = false;
            if (noOfElements - trailingNulls > 1) {
                while (indexArr.hasRemaining()) {
                    if (countOfElementsRead < (noOfElements - trailingNulls)) {
                        if (currOff == -1) {
                            if ((indexArr.position() + 2 * baseSize) <= indexArr.capacity()) {
                                if (useShort) {
                                    currOff = indexArr.getShort() + Short.MAX_VALUE;
                                    nextOff = indexArr.getShort() + Short.MAX_VALUE;
                                } else {
                                    currOff = indexArr.getInt();
                                    nextOff = indexArr.getInt();
                                }
                                countOfElementsRead += 2;
                            }
                        } else {
                            currOff = nextOff;
                            if (useShort) {
                                nextOff = indexArr.getShort() + Short.MAX_VALUE;
                            } else {
                                nextOff = indexArr.getInt();
                            }
                            countOfElementsRead += 1;
                        }
                        if (nextOff != currOff && foundNull) {
                            foundNull = false;
                            currOff += 2;
                        }
                        int elementLength = nextOff - currOff;
                        if (elementLength == 0) {
                            // Null element
                            foundNull = true;
                            i++;
                            continue;
                        }
                        buffer.position(currOff + initPos);
                        // Subtract the seperator from the element length
                        byte[] val = new byte[elementLength - 1];
                        buffer.get(val);
                        elements[i++] = baseDataType.toObject(val, sortOrder);
                    }
                }
                // Last before element was null
                if (foundNull) {
                    nextOff += 2;
                    foundNull = false;
                }
                // Last element was null
                buffer.position(nextOff + initPos);
                if (indexOffset - nextOff != 0) {
                    // Remove the seperator of the last element and the last two seperator bytes
                    byte[] val = new byte[(indexOffset - (3 * Bytes.SIZEOF_BYTE)) - nextOff];
                    buffer.get(val);
                    elements[i++] = baseDataType.toObject(val, sortOrder);
                }
            } else {
                buffer.position(initPos);
                // Remove the seperator of the last element and the last two seperator bytes
                if ((indexOffset + initPos) - valArrayPostion != 0) {
                    byte[] val = new byte[(indexOffset - (3 * Bytes.SIZEOF_BYTE) + initPos) - valArrayPostion];
                    buffer.position(valArrayPostion);
                    buffer.get(val);
                    elements[i++] = baseDataType.toObject(val, sortOrder);
                }
            }
        } else {
            buffer.position(initPos);
            for (int i = 0; i < noOfElements; i++) {
                byte[] val;
                if (baseDataType.getByteSize() == null) {
                    val = new byte[length];
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
            return size;
        }
        
    }

}