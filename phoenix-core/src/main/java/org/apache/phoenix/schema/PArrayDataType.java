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
 * The datatype for PColummns that are Arrays. 
 * Any variable length array would follow the below order.
 * Every element would be seperated by a seperator byte '0'. 
 * Null elements are counted and once a first non null element appears we write the
 * count of the nulls prefixed with a seperator byte Trailing nulls are not taken into account. 
 * The last non null element is followed by two seperator bytes. 
 * For eg a, b, null, null, c, null -> 65 0 66 0 0 2 67 0 0 0 
 * a null null null b c null d -> 65 0 0 3 66 0 67 0 0 1 68 0 0 0
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
		if (!baseType.isFixedWidth() || baseType.isCoercibleTo(PDataType.VARCHAR)) {
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
            do {
                byte nNullsWritten = (byte)(nulls % 256);
                oStream.write(nNullsWritten); // Single byte for repeating nulls
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
	
	public static void positionAtArrayElement(ImmutableBytesWritable ptr, int arrayIndex, PDataType baseDataType) {
		byte[] bytes = ptr.get();
		int initPos = ptr.getOffset();
		int noOfElements = 0;
		noOfElements = Bytes.toInt(bytes, (ptr.getOffset() + ptr.getLength() - ( Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)), Bytes.SIZEOF_INT);
		
		if(arrayIndex >= noOfElements) {
			throw new IndexOutOfBoundsException(
					"Invalid index "
							+ arrayIndex
							+ " specified, greater than the no of eloements in the array: "
							+ noOfElements);
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
                    (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + 2 * Bytes.SIZEOF_INT))) + ptr.getOffset();
		    int valArrayPostion = offset;
			int currOff = 0;
			if (noOfElements > 1) {
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
							ptr.set(bytes, currOff + initPos, 1);
							if(ptr.compareTo(QueryConstants.SEPARATOR_BYTE_ARRAY) == 0) {
							    // null found
							    currOff+=2;
							}
							nextOff = indexOffset;
							offset += baseSize;
						} else {
							currOff = Bytes.toShort(bytes, offset, baseSize) + Short.MAX_VALUE;
							ptr.set(bytes, currOff + initPos, 1);
							offset += baseSize;
							nextOff = Bytes.toShort(bytes, offset, baseSize) + Short.MAX_VALUE;
							offset += baseSize;
							if(ptr.compareTo(QueryConstants.SEPARATOR_BYTE_ARRAY) == 0) {
							    if(nextOff == currOff) {
							        // null found
							        ptr.set(bytes, currOff + initPos, 0);
							        return;
							    }
							    currOff += 2;
                            }
						}
					} else {
						// If the arrayIndex is already the last element then read the last before one element and the last element
						offset = indexOffset + (Bytes.SIZEOF_INT * arrayIndex);
						if (arrayIndex == (noOfElements - 1)) {
							currOff = Bytes.toInt(bytes, offset, baseSize);
							ptr.set(bytes, currOff + initPos, 1);
							if(ptr.compareTo(QueryConstants.SEPARATOR_BYTE_ARRAY) == 0) {
                                // null found
                                currOff+=2;
                            }
							nextOff = indexOffset;
							offset += baseSize;
						} else {
							currOff = Bytes.toInt(bytes, offset, baseSize);
							ptr.set(bytes, currOff + initPos, 1);
							offset += baseSize;
							nextOff = Bytes.toInt(bytes, offset, baseSize);
							offset += baseSize;
                            if (ptr.compareTo(QueryConstants.SEPARATOR_BYTE_ARRAY) == 0) {
                                if (nextOff == currOff) {
                                    // null found
                                    ptr.set(bytes, currOff + initPos, 0);
                                    return;
                                }
                                currOff +=2;
                            } 
						}
					}
					int elementLength = nextOff - currOff;
					if(elementLength == 0) {
					    // Means a null element
					    ptr.set(bytes, currOff + initPos, elementLength);
					    break;
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
            if (!baseType.isFixedWidth() || baseType.isCoercibleTo(PDataType.VARCHAR)) {
                int[] offsetPos = new int[noOfElements];
                int nulls = 0;
                for (int i = 0; i < noOfElements; i++) {
                    byte[] bytes = array.toBytes(i);
                    offsetPos[i] = byteStream.size();
                    if (bytes.length == 0) {
                        nulls++;
                    } else {
                        nulls = serializeNulls(oStream, nulls);
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
                    oStream.write(bytes, 0, bytes.length);
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
	private Object createPhoenixArray(byte[] bytes, int offset, int length,
			SortOrder sortOrder, PDataType baseDataType) {
		if(bytes == null || bytes.length == 0) {
			return null;
		}
		ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
		int initPos = buffer.position();
		buffer.position((buffer.limit() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)));
		int noOfElements = buffer.getInt();
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
            buffer.position(buffer.limit() - (Bytes.SIZEOF_BYTE + (2 * Bytes.SIZEOF_INT)));
            int indexOffset = buffer.getInt();
            buffer.position(initPos);
            int valArrayPostion = buffer.position();
            buffer.position(indexOffset + initPos);
            ByteBuffer indexArr = ByteBuffer.allocate(initOffsetArray(noOfElements, baseSize));
            byte[] array = indexArr.array();
            buffer.get(array);
            int countOfElementsRead = 0;
            int i = 0;
            int currOff = -1;
            int nextOff = -1;
            boolean foundNull = false;
            if (noOfElements > 1) {
                while (indexArr.hasRemaining()) {
                    if (countOfElementsRead < (noOfElements)) {
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
                        // If a non null element appears after a null.
                        // We would have written the null count prefixed with a seperator
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
                if(buffer.get(nextOff) == QueryConstants.SEPARATOR_BYTE) {
                    // We have got the required elements.  This would help in cases where we have a null
                    // last element
                    return PArrayDataType.instantiatePhoenixArray(baseDataType, elements);
                }
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