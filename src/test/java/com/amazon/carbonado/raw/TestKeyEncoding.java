/*
 * Copyright 2006-2010 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.carbonado.raw;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Random;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Test case for {@link KeyEncoder} and {@link KeyDecoder}. 
 * <p>
 * It generates random data values, checks that the decoding produces the
 * original results, and it checks that the order of the encoded bytes matches
 * the order of the original data values.
 *
 * @author Brian S O'Neill
 */
public class TestKeyEncoding extends TestCase {
    private static final int SHORT_TEST = 100;
    private static final int MEDIUM_TEST = 1000;
    private static final int LONG_TEST = 10000;
    private static final int LONG_LONG_TEST = 100000;

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestKeyEncoding.class);
    }

    private final long mSeed;

    private Random mRandom;

    public TestKeyEncoding(String name) {
        super(name);
        mSeed = 5399777425345431L;
    }

    protected void setUp() {
        mRandom = new Random(mSeed);
    }

    protected void tearDown() {
    }

    public void test_booleanDesc() throws Exception {
        byte[] bytes = new byte[1];
        boolean lastValue = false;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            boolean value = mRandom.nextBoolean();
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeBooleanDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_BooleanDesc() throws Exception {
        byte[] bytes = new byte[1];
        Boolean lastValue = false;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Boolean value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                value = mRandom.nextBoolean();
            }
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeBooleanObjDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_byteDesc() throws Exception {
        byte[] bytes = new byte[1];
        byte lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            byte value = (byte) mRandom.nextInt();
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeByteDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_ByteDesc() throws Exception {
        byte[] bytes = new byte[2];
        Byte lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Byte value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, KeyEncoder.encodeDesc(value, bytes, 0));
            } else {
                value = (byte) mRandom.nextInt();
                assertEquals(2, KeyEncoder.encodeDesc(value, bytes, 0));
            }
            assertEquals(value, KeyDecoder.decodeByteObjDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_shortDesc() throws Exception {
        byte[] bytes = new byte[2];
        short lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            short value = (short) mRandom.nextInt();
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeShortDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_ShortDesc() throws Exception {
        byte[] bytes = new byte[3];
        Short lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Short value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, KeyEncoder.encodeDesc(value, bytes, 0));
            } else {
                value = (short) mRandom.nextInt();
                assertEquals(3, KeyEncoder.encodeDesc(value, bytes, 0));
            }
            assertEquals(value, KeyDecoder.decodeShortObjDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_charDesc() throws Exception {
        byte[] bytes = new byte[2];
        char lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            char value = (char) mRandom.nextInt();
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeCharDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_CharacterDesc() throws Exception {
        byte[] bytes = new byte[3];
        Character lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Character value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, KeyEncoder.encodeDesc(value, bytes, 0));
            } else {
                value = (char) mRandom.nextInt();
                assertEquals(3, KeyEncoder.encodeDesc(value, bytes, 0));
            }
            assertEquals(value, KeyDecoder.decodeCharacterObjDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_intDesc() throws Exception {
        byte[] bytes = new byte[4];
        int lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            int value = mRandom.nextInt();
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeIntDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_IntegerDesc() throws Exception {
        byte[] bytes = new byte[5];
        Integer lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Integer value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, KeyEncoder.encodeDesc(value, bytes, 0));
            } else {
                value = mRandom.nextInt();
                assertEquals(5, KeyEncoder.encodeDesc(value, bytes, 0));
            }
            assertEquals(value, KeyDecoder.decodeIntegerObjDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_longDesc() throws Exception {
        byte[] bytes = new byte[8];
        long lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            long value = mRandom.nextLong();
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeLongDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_LongDesc() throws Exception {
        byte[] bytes = new byte[9];
        Long lastValue = 0L;
        byte[] lastBytes = null;
        for (int i=0; i<SHORT_TEST; i++) {
            Long value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
                assertEquals(1, KeyEncoder.encodeDesc(value, bytes, 0));
            } else {
                value = mRandom.nextLong();
                assertEquals(9, KeyEncoder.encodeDesc(value, bytes, 0));
            }
            assertEquals(value, KeyDecoder.decodeLongObjDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_floatDesc() throws Exception {
        byte[] bytes = new byte[4];
        float lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            float value = Float.intBitsToFloat(mRandom.nextInt());
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeFloatDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_FloatDesc() throws Exception {
        byte[] bytes = new byte[4];
        Float lastValue = 0f;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            Float value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                value = Float.intBitsToFloat(mRandom.nextInt());
            }
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeFloatObjDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_doubleDesc() throws Exception {
        byte[] bytes = new byte[8];
        double lastValue = 0;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            double value = Double.longBitsToDouble(mRandom.nextLong());
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeDoubleDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_DoubleDesc() throws Exception {
        byte[] bytes = new byte[8];
        Double lastValue = 0d;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            Double value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                value = Double.longBitsToDouble(mRandom.nextLong());
            }
            KeyEncoder.encodeDesc(value, bytes, 0);
            assertEquals(value, KeyDecoder.decodeDoubleObjDesc(bytes, 0));
            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_BigInteger() throws Exception {
        byte[] bytes = new byte[205];
        BigInteger[] ref = new BigInteger[1];

        BigInteger lastValue = null;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            BigInteger value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                int len = mRandom.nextInt(200 * 8);
                value = new BigInteger(len, mRandom);
                if (mRandom.nextBoolean()) {
                    value = value.negate();
                }
            }

            int amt = KeyEncoder.calculateEncodedLength(value);
            assertEquals(amt, KeyEncoder.encode(value, bytes, 0));
            assertEquals(amt, KeyDecoder.decode(bytes, 0, ref));
            assertEquals(value, ref[0]);

            if (lastBytes != null) {
                int sgn = TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_BigIntegerDesc() throws Exception {
        byte[] bytes = new byte[205];
        BigInteger[] ref = new BigInteger[1];

        BigInteger lastValue = null;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_TEST; i++) {
            BigInteger value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                int len = mRandom.nextInt(200 * 8);
                value = new BigInteger(len, mRandom);
                if (mRandom.nextBoolean()) {
                    value = value.negate();
                }
            }

            int amt = KeyEncoder.calculateEncodedLength(value);
            assertEquals(amt, KeyEncoder.encodeDesc(value, bytes, 0));
            assertEquals(amt, KeyDecoder.decodeDesc(bytes, 0, ref));
            assertEquals(value, ref[0]);

            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_BigDecimal() throws Exception {
        test_BigDecimal(false);
    }

    public void test_BigDecimalDesc() throws Exception {
        test_BigDecimal(true);
    }

    private void test_BigDecimal(boolean desc) throws Exception {
        BigDecimal[] ref = new BigDecimal[1];

        BigDecimal[] values = {
            new BigDecimal("-123.0"),
            new BigDecimal("-123"),
            new BigDecimal("-90.01000"),
            new BigDecimal("-90.0100"),
            new BigDecimal("-90.010"),
            new BigDecimal("-90.01"),
            new BigDecimal("-11.000"),
            new BigDecimal("-11.00"),
            new BigDecimal("-11.0"),
            new BigDecimal("-11"),
            new BigDecimal("-10.100"),
            new BigDecimal("-10.10"),
            new BigDecimal("-10.1"),
            new BigDecimal("-10.010"),
            new BigDecimal("-10.01"),
            new BigDecimal("-10.001"),
            new BigDecimal("-2"),
            new BigDecimal("-1"),
            new BigDecimal("-0.1"),
            new BigDecimal("-0.01"),
            new BigDecimal("-0.001"),
            new BigDecimal("-0.0001"),
            new BigDecimal("-0.00001"),
            BigDecimal.ZERO,
            new BigDecimal("0.00001"),
            new BigDecimal("0.0001"),
            new BigDecimal("0.001"),
            new BigDecimal("0.01"),
            new BigDecimal("0.1"),
            new BigDecimal("1"),
            new BigDecimal("2"),
            new BigDecimal("10.001"),
            new BigDecimal("10.01"),
            new BigDecimal("10.010"),
            new BigDecimal("10.1"),
            new BigDecimal("10.10"),
            new BigDecimal("10.100"),
            new BigDecimal("11"),
            new BigDecimal("11.0"),
            new BigDecimal("11.00"),
            new BigDecimal("11.000"),
            new BigDecimal("90.01"),
            new BigDecimal("90.010"),
            new BigDecimal("90.0100"),
            new BigDecimal("90.01000"),
            new BigDecimal("123"),
            new BigDecimal("123.0"),
        };

        byte[][] encoded = new byte[values.length][];

        for (int i=0; i<values.length; i++) {
            encoded[i] = new byte[KeyEncoder.calculateEncodedLength(values[i])];
            if (desc) {
                KeyEncoder.encodeDesc(values[i], encoded[i], 0);
            } else {
                KeyEncoder.encode(values[i], encoded[i], 0);
            }
        }

        for (int i=0; i<values.length; i++) {
            ref[0] = BigDecimal.ZERO;
            if (desc) {
                KeyDecoder.decodeDesc(encoded[i], 0, ref);
            } else {
                KeyDecoder.decode(encoded[i], 0, ref);
            }
            assertEquals(values[i], ref[0]);
        }

        for (int i=1; i<values.length; i++) {
            int sgn = TestDataEncoding.byteArrayCompare(encoded[i - 1], encoded[i]);
            if (desc) {
                assertTrue(sgn > 0);
            } else {
                assertTrue(sgn < 0);
            }
        }
    }

    public void test_BigDecimalRandom() throws Exception {
        test_BigDecimalRandom(false);
    }

    public void test_BigDecimalRandomDesc() throws Exception {
        test_BigDecimalRandom(true);
    }

    private void test_BigDecimalRandom(boolean desc) throws Exception {
        BigDecimal[] ref = new BigDecimal[1];

        byte[] bytes = new byte[1000];

        BigDecimal lastValue = null;
        byte[] lastBytes = null;
        for (int i=0; i<LONG_LONG_TEST; i++) {
            BigDecimal value;
            int mode = mRandom.nextInt(20);
            if (mode == 1) {
                value = null;
            } else if (mode == 2) {
                int len = mRandom.nextInt(10 * 8);
                BigInteger unscaled = new BigInteger(len, mRandom);
                if (mRandom.nextBoolean()) {
                    unscaled = unscaled.negate();
                }
                int scale = mRandom.nextInt(10) - 5;
                value = new BigDecimal(unscaled, scale);
            } else {
                int len = mRandom.nextInt(100 * 8);
                BigInteger unscaled = new BigInteger(len, mRandom);
                if (mRandom.nextBoolean()) {
                    unscaled = unscaled.negate();
                }
                int scale = mRandom.nextInt();
                value = new BigDecimal(unscaled, scale);
            }

            int amt = KeyEncoder.calculateEncodedLength(value);
            int actualAmt;
            if (desc) {
                actualAmt = KeyEncoder.encodeDesc(value, bytes, 0);
            } else {
                actualAmt = KeyEncoder.encode(value, bytes, 0);
            }
            assertEquals(amt, actualAmt);

            if (desc) {
                actualAmt = KeyDecoder.decodeDesc(bytes, 0, ref);
            } else {
                actualAmt = KeyDecoder.decode(bytes, 0, ref);
            }
            if (value != null && value.compareTo(BigDecimal.ZERO) == 0) {
                assertTrue(value.compareTo(ref[0]) == 0);
            } else {
                assertEquals(value, ref[0]);
            }
            assertEquals(amt, actualAmt);

            if (lastBytes != null) {
                int sgn = TestDataEncoding.compare(value, lastValue);
                if (desc) {
                    sgn = -sgn;
                }
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_String() throws Exception {
        String lastValue = null;
        byte[] lastBytes = null;
        String[] ref = new String[1];
        for (int i=0; i<SHORT_TEST; i++) {
            String value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                int length;
                switch (mRandom.nextInt(15)) {
                default:
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    length = mRandom.nextInt(100);
                    break;
                case 8: case 9: case 10: case 11:
                    length = mRandom.nextInt(200);
                    break;
                case 12: case 13:
                    length = mRandom.nextInt(20000);
                    break;
                case 14:
                    length = mRandom.nextInt(3000000);
                    break;
                }
                char[] chars = new char[length];
                for (int j=0; j<length; j++) {
                    char c;
                    switch (mRandom.nextInt(7)) {
                    default:
                    case 0: case 1: case 2: case 3:
                        c = (char) mRandom.nextInt(128);
                        break;
                    case 4: case 5:
                        c = (char) mRandom.nextInt(4000);
                        break;
                    case 6:
                        c = (char) mRandom.nextInt();
                        break;
                    }
                    chars[j] = c;
                }
                value = new String(chars);
            }

            byte[] bytes = new byte[KeyEncoder.calculateEncodedStringLength(value)];
            assertEquals(bytes.length, KeyEncoder.encode(value, bytes, 0));
            assertEquals(bytes.length, KeyDecoder.decodeString(bytes, 0, ref));
            assertEquals(value, ref[0]);

            if (lastBytes != null) {
                int sgn = TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_StringDesc() throws Exception {
        String lastValue = null;
        byte[] lastBytes = null;
        String[] ref = new String[1];
        for (int i=0; i<SHORT_TEST; i++) {
            String value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                int length;
                switch (mRandom.nextInt(15)) {
                default:
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    length = mRandom.nextInt(100);
                    break;
                case 8: case 9: case 10: case 11:
                    length = mRandom.nextInt(200);
                    break;
                case 12: case 13:
                    length = mRandom.nextInt(20000);
                    break;
                case 14:
                    length = mRandom.nextInt(3000000);
                    break;
                }
                char[] chars = new char[length];
                for (int j=0; j<length; j++) {
                    char c;
                    switch (mRandom.nextInt(7)) {
                    default:
                    case 0: case 1: case 2: case 3:
                        c = (char) mRandom.nextInt(128);
                        break;
                    case 4: case 5:
                        c = (char) mRandom.nextInt(4000);
                        break;
                    case 6:
                        c = (char) mRandom.nextInt();
                        break;
                    }
                    chars[j] = c;
                }
                value = new String(chars);
            }

            byte[] bytes = new byte[KeyEncoder.calculateEncodedStringLength(value)];
            assertEquals(bytes.length, KeyEncoder.encodeDesc(value, bytes, 0));
            assertEquals(bytes.length, KeyDecoder.decodeStringDesc(bytes, 0, ref));
            assertEquals(value, ref[0]);


            if (lastBytes != null) {
                int sgn = -TestDataEncoding.compare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_byteArray() throws Exception {
        byte[] lastValue = null;
        byte[] lastBytes = null;
        byte[][] ref = new byte[1][];
        for (int i=0; i<LONG_TEST; i++) {
            byte[] value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                int length = mRandom.nextInt(4000);
                value = new byte[length];
                for (int j=0; j<length; j++) {
                    value[j] = (byte) mRandom.nextInt();
                }
            }

            byte[] bytes = new byte[KeyEncoder.calculateEncodedLength(value)];
            assertEquals(bytes.length, KeyEncoder.encode(value, bytes, 0));
            assertEquals(bytes.length, KeyDecoder.decode(bytes, 0, ref));
            if (ref[0] == null) {
                assertEquals(value, null);
            } else if (value == null) {
                assertEquals(value, ref[0]);
            } else {
                assertEquals(0, TestDataEncoding.byteArrayCompare(value, ref[0], value.length));
            }

            if (lastBytes != null) {
                int sgn = TestDataEncoding.byteArrayOrNullCompare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }

    public void test_byteArrayDesc() throws Exception {
        byte[] lastValue = null;
        byte[] lastBytes = null;
        byte[][] ref = new byte[1][];
        for (int i=0; i<LONG_TEST; i++) {
            byte[] value;
            if (mRandom.nextInt(10) == 1) {
                value = null;
            } else {
                int length = mRandom.nextInt(4000);
                value = new byte[length];
                for (int j=0; j<length; j++) {
                    value[j] = (byte) mRandom.nextInt();
                }
            }

            byte[] bytes = new byte[KeyEncoder.calculateEncodedLength(value)];
            assertEquals(bytes.length, KeyEncoder.encodeDesc(value, bytes, 0));
            assertEquals(bytes.length, KeyDecoder.decodeDesc(bytes, 0, ref));
            if (ref[0] == null) {
                assertEquals(value, null);
            } else if (value == null) {
                assertEquals(value, ref[0]);
            } else {
                assertEquals(0, TestDataEncoding.byteArrayCompare(value, ref[0], value.length));
            }

            if (lastBytes != null) {
                int sgn = -TestDataEncoding.byteArrayOrNullCompare(value, lastValue);
                assertEquals(sgn, TestDataEncoding.byteArrayCompare(bytes, lastBytes));
            }
            lastValue = value;
            lastBytes = bytes.clone();
        }
    }
}
