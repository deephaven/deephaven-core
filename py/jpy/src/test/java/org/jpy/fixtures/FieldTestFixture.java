/*
 * Copyright 2015 Brockmann Consult GmbH
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

package org.jpy.fixtures;

/**
 * Used as a test class for the test cases in jpy_field_test.py
 *
 * @author Norman Fomferra
 */
@SuppressWarnings("UnusedDeclaration")
public class FieldTestFixture {

    public static final boolean z_STATIC_FIELD = true;
    public static final char c_STATIC_FIELD = 'A';
    public static final byte b_STATIC_FIELD = (byte) 123;
    public static final short s_STATIC_FIELD = (short) 12345;
    public static final int i_STATIC_FIELD = 123456789;
    public static final long j_STATIC_FIELD = 1234567890123456789L;
    public static final float f_STATIC_FIELD = 0.12345F;
    public static final double d_STATIC_FIELD = 0.123456789;

    public static final String S_OBJ_STATIC_FIELD = "ABC";
    public static final Thing l_OBJ_STATIC_FIELD = new Thing(123);

    public boolean zInstField;
    public char cInstField;
    public byte bInstField;
    public short sInstField;
    public int iInstField;
    public long jInstField;
    public float fInstField;
    public double dInstField;

    public Boolean zObjInstField;
    public Character cObjInstField;
    public Byte bObjInstField;
    public Short sObjInstField;
    public Integer iObjInstField;
    public Long jObjInstField;
    public Float fObjInstField;
    public Double dObjInstField;

    public String SObjInstField;
    public Thing lObjInstField;
}
