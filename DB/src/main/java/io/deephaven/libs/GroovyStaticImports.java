/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - Run GroovyStaticImportGenerator or "./gradlew :Generators:groovyStaticImportGenerator" to regenerate
 ****************************************************************************************************************************/

package io.deephaven.libs;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbBooleanArray;
import io.deephaven.db.tables.dbarrays.DbByteArray;
import io.deephaven.db.tables.dbarrays.DbCharArray;
import io.deephaven.db.tables.dbarrays.DbDoubleArray;
import io.deephaven.db.tables.dbarrays.DbFloatArray;
import io.deephaven.db.tables.dbarrays.DbIntArray;
import io.deephaven.db.tables.dbarrays.DbLongArray;
import io.deephaven.db.tables.dbarrays.DbShortArray;
import io.deephaven.libs.primitives.BinSearch;
import io.deephaven.libs.primitives.BooleanPrimitives;
import io.deephaven.libs.primitives.ByteNumericPrimitives;
import io.deephaven.libs.primitives.BytePrimitives;
import io.deephaven.libs.primitives.Casting;
import io.deephaven.libs.primitives.CharacterPrimitives;
import io.deephaven.libs.primitives.ComparePrimitives;
import io.deephaven.libs.primitives.DoubleFpPrimitives;
import io.deephaven.libs.primitives.DoubleNumericPrimitives;
import io.deephaven.libs.primitives.DoublePrimitives;
import io.deephaven.libs.primitives.FloatFpPrimitives;
import io.deephaven.libs.primitives.FloatNumericPrimitives;
import io.deephaven.libs.primitives.FloatPrimitives;
import io.deephaven.libs.primitives.IntegerNumericPrimitives;
import io.deephaven.libs.primitives.IntegerPrimitives;
import io.deephaven.libs.primitives.LongNumericPrimitives;
import io.deephaven.libs.primitives.LongPrimitives;
import io.deephaven.libs.primitives.ObjectPrimitives;
import io.deephaven.libs.primitives.ShortNumericPrimitives;
import io.deephaven.libs.primitives.ShortPrimitives;
import io.deephaven.libs.primitives.SpecialPrimitives;
import java.lang.Boolean;
import java.lang.Byte;
import java.lang.CharSequence;
import java.lang.Character;
import java.lang.Comparable;
import java.lang.Double;
import java.lang.Float;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Short;
import java.lang.String;

/**
 * Functions statically imported into Groovy.
 *
 * @see io.deephaven.libs.primitives
 */
public class GroovyStaticImports {
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#abs(byte) */
    public static  byte abs( byte value ) {return ByteNumericPrimitives.abs( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#abs(double) */
    public static  double abs( double value ) {return DoubleNumericPrimitives.abs( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#abs(float) */
    public static  float abs( float value ) {return FloatNumericPrimitives.abs( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#abs(int) */
    public static  int abs( int value ) {return IntegerNumericPrimitives.abs( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#abs(long) */
    public static  long abs( long value ) {return LongNumericPrimitives.abs( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#abs(short) */
    public static  short abs( short value ) {return ShortNumericPrimitives.abs( value );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#absAvg(byte[]) */
    public static  double absAvg( byte[] values ) {return ByteNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#absAvg(double[]) */
    public static  double absAvg( double[] values ) {return DoubleNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#absAvg(float[]) */
    public static  double absAvg( float[] values ) {return FloatNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#absAvg(int[]) */
    public static  double absAvg( int[] values ) {return IntegerNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#absAvg(long[]) */
    public static  double absAvg( long[] values ) {return LongNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#absAvg(java.lang.Byte[]) */
    public static  double absAvg( java.lang.Byte[] values ) {return ByteNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#absAvg(java.lang.Double[]) */
    public static  double absAvg( java.lang.Double[] values ) {return DoubleNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#absAvg(java.lang.Float[]) */
    public static  double absAvg( java.lang.Float[] values ) {return FloatNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#absAvg(java.lang.Integer[]) */
    public static  double absAvg( java.lang.Integer[] values ) {return IntegerNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#absAvg(java.lang.Long[]) */
    public static  double absAvg( java.lang.Long[] values ) {return LongNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#absAvg(java.lang.Short[]) */
    public static  double absAvg( java.lang.Short[] values ) {return ShortNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#absAvg(short[]) */
    public static  double absAvg( short[] values ) {return ShortNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#absAvg(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double absAvg( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#absAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double absAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#absAvg(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double absAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#absAvg(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double absAvg( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#absAvg(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double absAvg( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#absAvg(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double absAvg( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#acos(byte) */
    public static  double acos( byte value ) {return ByteNumericPrimitives.acos( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#acos(double) */
    public static  double acos( double value ) {return DoubleNumericPrimitives.acos( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#acos(float) */
    public static  double acos( float value ) {return FloatNumericPrimitives.acos( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#acos(int) */
    public static  double acos( int value ) {return IntegerNumericPrimitives.acos( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#acos(long) */
    public static  double acos( long value ) {return LongNumericPrimitives.acos( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#acos(short) */
    public static  double acos( short value ) {return ShortNumericPrimitives.acos( value );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#and(java.lang.Boolean[]) */
    public static  java.lang.Boolean and( java.lang.Boolean[] values ) {return BooleanPrimitives.and( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#and(boolean[]) */
    public static  java.lang.Boolean and( boolean[] values ) {return BooleanPrimitives.and( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#and(io.deephaven.db.tables.dbarrays.DbArray<java.lang.Boolean>) */
    public static  java.lang.Boolean and( io.deephaven.db.tables.dbarrays.DbArray<java.lang.Boolean> values ) {return BooleanPrimitives.and( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#and(java.lang.Boolean[],java.lang.Boolean) */
    public static  java.lang.Boolean and( java.lang.Boolean[] values, java.lang.Boolean nullValue ) {return BooleanPrimitives.and( values, nullValue );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#and(io.deephaven.db.tables.dbarrays.DbArray<java.lang.Boolean>,java.lang.Boolean) */
    public static  java.lang.Boolean and( io.deephaven.db.tables.dbarrays.DbArray<java.lang.Boolean> values, java.lang.Boolean nullValue ) {return BooleanPrimitives.and( values, nullValue );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#array(byte[]) */
    public static  io.deephaven.db.tables.dbarrays.DbByteArray array( byte[] values ) {return BytePrimitives.array( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#array(char[]) */
    public static  io.deephaven.db.tables.dbarrays.DbCharArray array( char[] values ) {return CharacterPrimitives.array( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#array(double[]) */
    public static  io.deephaven.db.tables.dbarrays.DbDoubleArray array( double[] values ) {return DoublePrimitives.array( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#array(float[]) */
    public static  io.deephaven.db.tables.dbarrays.DbFloatArray array( float[] values ) {return FloatPrimitives.array( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#array(int[]) */
    public static  io.deephaven.db.tables.dbarrays.DbIntArray array( int[] values ) {return IntegerPrimitives.array( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#array(long[]) */
    public static  io.deephaven.db.tables.dbarrays.DbLongArray array( long[] values ) {return LongPrimitives.array( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#array(java.lang.Boolean[]) */
    public static  io.deephaven.db.tables.dbarrays.DbBooleanArray array( java.lang.Boolean[] values ) {return BooleanPrimitives.array( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#array(short[]) */
    public static  io.deephaven.db.tables.dbarrays.DbShortArray array( short[] values ) {return ShortPrimitives.array( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#asin(byte) */
    public static  double asin( byte value ) {return ByteNumericPrimitives.asin( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#asin(double) */
    public static  double asin( double value ) {return DoubleNumericPrimitives.asin( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#asin(float) */
    public static  double asin( float value ) {return FloatNumericPrimitives.asin( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#asin(int) */
    public static  double asin( int value ) {return IntegerNumericPrimitives.asin( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#asin(long) */
    public static  double asin( long value ) {return LongNumericPrimitives.asin( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#asin(short) */
    public static  double asin( short value ) {return ShortNumericPrimitives.asin( value );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#atan(byte) */
    public static  double atan( byte value ) {return ByteNumericPrimitives.atan( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#atan(double) */
    public static  double atan( double value ) {return DoubleNumericPrimitives.atan( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#atan(float) */
    public static  double atan( float value ) {return FloatNumericPrimitives.atan( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#atan(int) */
    public static  double atan( int value ) {return IntegerNumericPrimitives.atan( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#atan(long) */
    public static  double atan( long value ) {return LongNumericPrimitives.atan( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#atan(short) */
    public static  double atan( short value ) {return ShortNumericPrimitives.atan( value );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#avg(byte[]) */
    public static  double avg( byte[] values ) {return ByteNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#avg(double[]) */
    public static  double avg( double[] values ) {return DoubleNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#avg(float[]) */
    public static  double avg( float[] values ) {return FloatNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#avg(int[]) */
    public static  double avg( int[] values ) {return IntegerNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#avg(long[]) */
    public static  double avg( long[] values ) {return LongNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#avg(java.lang.Byte[]) */
    public static  double avg( java.lang.Byte[] values ) {return ByteNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#avg(java.lang.Double[]) */
    public static  double avg( java.lang.Double[] values ) {return DoubleNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#avg(java.lang.Float[]) */
    public static  double avg( java.lang.Float[] values ) {return FloatNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#avg(java.lang.Integer[]) */
    public static  double avg( java.lang.Integer[] values ) {return IntegerNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#avg(java.lang.Long[]) */
    public static  double avg( java.lang.Long[] values ) {return LongNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#avg(java.lang.Short[]) */
    public static  double avg( java.lang.Short[] values ) {return ShortNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#avg(short[]) */
    public static  double avg( short[] values ) {return ShortNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#avg(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double avg( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#avg(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double avg( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#avg(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double avg( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#avg(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double avg( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#avg(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double avg( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#avg(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double avg( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.avg( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#binSearchIndex(byte[],byte,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( byte[] values, byte key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ByteNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#binSearchIndex(double[],double,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( double[] values, double key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return DoubleNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#binSearchIndex(float[],float,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( float[] values, float key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return FloatNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#binSearchIndex(int[],int,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( int[] values, int key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return IntegerNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#binSearchIndex(long[],long,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( long[] values, long key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return LongNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#binSearchIndex(short[],short,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( short[] values, short key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ShortNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#binSearchIndex(io.deephaven.db.tables.dbarrays.DbByteArray,byte,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( io.deephaven.db.tables.dbarrays.DbByteArray values, byte key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ByteNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#binSearchIndex(io.deephaven.db.tables.dbarrays.DbDoubleArray,double,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return DoubleNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#binSearchIndex(io.deephaven.db.tables.dbarrays.DbFloatArray,float,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( io.deephaven.db.tables.dbarrays.DbFloatArray values, float key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return FloatNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#binSearchIndex(io.deephaven.db.tables.dbarrays.DbIntArray,int,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( io.deephaven.db.tables.dbarrays.DbIntArray values, int key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return IntegerNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#binSearchIndex(io.deephaven.db.tables.dbarrays.DbLongArray,long,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( io.deephaven.db.tables.dbarrays.DbLongArray values, long key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return LongNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#binSearchIndex(io.deephaven.db.tables.dbarrays.DbShortArray,short,io.deephaven.libs.primitives.BinSearch) */
    public static  int binSearchIndex( io.deephaven.db.tables.dbarrays.DbShortArray values, short key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ShortNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#binSearchIndex(io.deephaven.db.tables.dbarrays.DbArray,T,io.deephaven.libs.primitives.BinSearch) */
    public static <T extends java.lang.Comparable<? super T>> int binSearchIndex( io.deephaven.db.tables.dbarrays.DbArray<T> values, T key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ObjectPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(byte[]) */
    public static  double[] castDouble( byte[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(double[]) */
    public static  double[] castDouble( double[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(float[]) */
    public static  double[] castDouble( float[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(int[]) */
    public static  double[] castDouble( int[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(long[]) */
    public static  double[] castDouble( long[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(short[]) */
    public static  double[] castDouble( short[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double[] castDouble( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double[] castDouble( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double[] castDouble( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double[] castDouble( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double[] castDouble( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castDouble(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double[] castDouble( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#castLong(byte[]) */
    public static  long[] castLong( byte[] values ) {return Casting.castLong( values );}
    /** @see io.deephaven.libs.primitives.Casting#castLong(int[]) */
    public static  long[] castLong( int[] values ) {return Casting.castLong( values );}
    /** @see io.deephaven.libs.primitives.Casting#castLong(long[]) */
    public static  long[] castLong( long[] values ) {return Casting.castLong( values );}
    /** @see io.deephaven.libs.primitives.Casting#castLong(short[]) */
    public static  long[] castLong( short[] values ) {return Casting.castLong( values );}
    /** @see io.deephaven.libs.primitives.Casting#castLong(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  long[] castLong( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return Casting.castLong( values );}
    /** @see io.deephaven.libs.primitives.Casting#castLong(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  long[] castLong( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return Casting.castLong( values );}
    /** @see io.deephaven.libs.primitives.Casting#castLong(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long[] castLong( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return Casting.castLong( values );}
    /** @see io.deephaven.libs.primitives.Casting#castLong(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  long[] castLong( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return Casting.castLong( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#ceil(byte) */
    public static  double ceil( byte value ) {return ByteNumericPrimitives.ceil( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#ceil(double) */
    public static  double ceil( double value ) {return DoubleNumericPrimitives.ceil( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#ceil(float) */
    public static  double ceil( float value ) {return FloatNumericPrimitives.ceil( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#ceil(int) */
    public static  double ceil( int value ) {return IntegerNumericPrimitives.ceil( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#ceil(long) */
    public static  double ceil( long value ) {return LongNumericPrimitives.ceil( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#ceil(short) */
    public static  double ceil( short value ) {return ShortNumericPrimitives.ceil( value );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#clamp(byte,byte,byte) */
    public static  byte clamp( byte value, byte min, byte max ) {return ByteNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#clamp(double,double,double) */
    public static  double clamp( double value, double min, double max ) {return DoubleNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#clamp(float,float,float) */
    public static  float clamp( float value, float min, float max ) {return FloatNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#clamp(int,int,int) */
    public static  int clamp( int value, int min, int max ) {return IntegerNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#clamp(long,long,long) */
    public static  long clamp( long value, long min, long max ) {return LongNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#clamp(short,short,short) */
    public static  short clamp( short value, short min, short max ) {return ShortNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#concat(io.deephaven.db.tables.dbarrays.DbByteArray[]) */
    public static  byte[] concat( io.deephaven.db.tables.dbarrays.DbByteArray[] values ) {return BytePrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#concat(io.deephaven.db.tables.dbarrays.DbCharArray[]) */
    public static  char[] concat( io.deephaven.db.tables.dbarrays.DbCharArray[] values ) {return CharacterPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#concat(io.deephaven.db.tables.dbarrays.DbDoubleArray[]) */
    public static  double[] concat( io.deephaven.db.tables.dbarrays.DbDoubleArray[] values ) {return DoublePrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#concat(io.deephaven.db.tables.dbarrays.DbFloatArray[]) */
    public static  float[] concat( io.deephaven.db.tables.dbarrays.DbFloatArray[] values ) {return FloatPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#concat(io.deephaven.db.tables.dbarrays.DbIntArray[]) */
    public static  int[] concat( io.deephaven.db.tables.dbarrays.DbIntArray[] values ) {return IntegerPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#concat(io.deephaven.db.tables.dbarrays.DbLongArray[]) */
    public static  long[] concat( io.deephaven.db.tables.dbarrays.DbLongArray[] values ) {return LongPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#concat(io.deephaven.db.tables.dbarrays.DbShortArray[]) */
    public static  short[] concat( io.deephaven.db.tables.dbarrays.DbShortArray[] values ) {return ShortPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#concat(byte[][]) */
    public static  byte[] concat( byte[][] values ) {return BytePrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#concat(char[][]) */
    public static  char[] concat( char[][] values ) {return CharacterPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#concat(double[][]) */
    public static  double[] concat( double[][] values ) {return DoublePrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#concat(float[][]) */
    public static  float[] concat( float[][] values ) {return FloatPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#concat(int[][]) */
    public static  int[] concat( int[][] values ) {return IntegerPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#concat(long[][]) */
    public static  long[] concat( long[][] values ) {return LongPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#concat(short[][]) */
    public static  short[] concat( short[][] values ) {return ShortPrimitives.concat( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#contains(java.lang.String,java.lang.CharSequence) */
    public static  boolean contains( java.lang.String target, java.lang.CharSequence sequence ) {return ObjectPrimitives.contains( target, sequence );}
    /** @see io.deephaven.libs.primitives.DoubleFpPrimitives#containsNonNormal(double[]) */
    public static  boolean containsNonNormal( double[] values ) {return DoubleFpPrimitives.containsNonNormal( values );}
    /** @see io.deephaven.libs.primitives.FloatFpPrimitives#containsNonNormal(float[]) */
    public static  boolean containsNonNormal( float[] values ) {return FloatFpPrimitives.containsNonNormal( values );}
    /** @see io.deephaven.libs.primitives.DoubleFpPrimitives#containsNonNormal(java.lang.Double[]) */
    public static  boolean containsNonNormal( java.lang.Double[] values ) {return DoubleFpPrimitives.containsNonNormal( values );}
    /** @see io.deephaven.libs.primitives.FloatFpPrimitives#containsNonNormal(java.lang.Float[]) */
    public static  boolean containsNonNormal( java.lang.Float[] values ) {return FloatFpPrimitives.containsNonNormal( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cor(byte[],byte[]) */
    public static  double cor( byte[] values0, byte[] values1 ) {return ByteNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cor(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double cor( byte[] values0, io.deephaven.db.tables.dbarrays.DbByteArray values1 ) {return ByteNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cor(double[],double[]) */
    public static  double cor( double[] values0, double[] values1 ) {return DoubleNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cor(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double cor( double[] values0, io.deephaven.db.tables.dbarrays.DbDoubleArray values1 ) {return DoubleNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cor(float[],float[]) */
    public static  double cor( float[] values0, float[] values1 ) {return FloatNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cor(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double cor( float[] values0, io.deephaven.db.tables.dbarrays.DbFloatArray values1 ) {return FloatNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cor(int[],int[]) */
    public static  double cor( int[] values0, int[] values1 ) {return IntegerNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cor(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double cor( int[] values0, io.deephaven.db.tables.dbarrays.DbIntArray values1 ) {return IntegerNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cor(long[],long[]) */
    public static  double cor( long[] values0, long[] values1 ) {return LongNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cor(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double cor( long[] values0, io.deephaven.db.tables.dbarrays.DbLongArray values1 ) {return LongNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cor(short[],short[]) */
    public static  double cor( short[] values0, short[] values1 ) {return ShortNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cor(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double cor( short[] values0, io.deephaven.db.tables.dbarrays.DbShortArray values1 ) {return ShortNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbByteArray values0, byte[] values1 ) {return ByteNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbByteArray values0, io.deephaven.db.tables.dbarrays.DbByteArray values1 ) {return ByteNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbDoubleArray values0, double[] values1 ) {return DoubleNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbDoubleArray values0, io.deephaven.db.tables.dbarrays.DbDoubleArray values1 ) {return DoubleNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbFloatArray values0, float[] values1 ) {return FloatNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbFloatArray values0, io.deephaven.db.tables.dbarrays.DbFloatArray values1 ) {return FloatNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbIntArray values0, int[] values1 ) {return IntegerNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbIntArray values0, io.deephaven.db.tables.dbarrays.DbIntArray values1 ) {return IntegerNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbLongArray values0, long[] values1 ) {return LongNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbLongArray values0, io.deephaven.db.tables.dbarrays.DbLongArray values1 ) {return LongNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbShortArray values0, short[] values1 ) {return ShortNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cor(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double cor( io.deephaven.db.tables.dbarrays.DbShortArray values0, io.deephaven.db.tables.dbarrays.DbShortArray values1 ) {return ShortNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cos(byte) */
    public static  double cos( byte value ) {return ByteNumericPrimitives.cos( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cos(double) */
    public static  double cos( double value ) {return DoubleNumericPrimitives.cos( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cos(float) */
    public static  double cos( float value ) {return FloatNumericPrimitives.cos( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cos(int) */
    public static  double cos( int value ) {return IntegerNumericPrimitives.cos( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cos(long) */
    public static  double cos( long value ) {return LongNumericPrimitives.cos( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cos(short) */
    public static  double cos( short value ) {return ShortNumericPrimitives.cos( value );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#count(byte[]) */
    public static  int count( byte[] values ) {return BytePrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#count(char[]) */
    public static  int count( char[] values ) {return CharacterPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#count(double[]) */
    public static  int count( double[] values ) {return DoublePrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#count(float[]) */
    public static  int count( float[] values ) {return FloatPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#count(int[]) */
    public static  int count( int[] values ) {return IntegerPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#count(long[]) */
    public static  int count( long[] values ) {return LongPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#count(java.lang.Byte[]) */
    public static  int count( java.lang.Byte[] values ) {return BytePrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#count(java.lang.Character[]) */
    public static  int count( java.lang.Character[] values ) {return CharacterPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#count(java.lang.Double[]) */
    public static  int count( java.lang.Double[] values ) {return DoublePrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#count(java.lang.Float[]) */
    public static  int count( java.lang.Float[] values ) {return FloatPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#count(java.lang.Integer[]) */
    public static  int count( java.lang.Integer[] values ) {return IntegerPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#count(java.lang.Long[]) */
    public static  int count( java.lang.Long[] values ) {return LongPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#count(java.lang.Short[]) */
    public static  int count( java.lang.Short[] values ) {return ShortPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#count(short[]) */
    public static  int count( short[] values ) {return ShortPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#count(io.deephaven.db.tables.dbarrays.DbBooleanArray) */
    public static  int count( io.deephaven.db.tables.dbarrays.DbBooleanArray values ) {return BooleanPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#count(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  int count( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return BytePrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#count(io.deephaven.db.tables.dbarrays.DbCharArray) */
    public static  int count( io.deephaven.db.tables.dbarrays.DbCharArray values ) {return CharacterPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#count(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  int count( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoublePrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#count(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  int count( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#count(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int count( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#count(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  int count( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#count(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  int count( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#count(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T> int count( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.count( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#countDistinct(byte[]) */
    public static  long countDistinct( byte[] values ) {return BytePrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#countDistinct(char[]) */
    public static  long countDistinct( char[] values ) {return CharacterPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#countDistinct(double[]) */
    public static  long countDistinct( double[] values ) {return DoublePrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#countDistinct(float[]) */
    public static  long countDistinct( float[] values ) {return FloatPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#countDistinct(int[]) */
    public static  long countDistinct( int[] values ) {return IntegerPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#countDistinct(long[]) */
    public static  long countDistinct( long[] values ) {return LongPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#countDistinct(short[]) */
    public static  long countDistinct( short[] values ) {return ShortPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return BytePrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbCharArray) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbCharArray values ) {return CharacterPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoublePrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T extends java.lang.Comparable<? super T>> long countDistinct( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.countDistinct( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#countDistinct(byte[],boolean) */
    public static  long countDistinct( byte[] values, boolean countNull ) {return BytePrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#countDistinct(char[],boolean) */
    public static  long countDistinct( char[] values, boolean countNull ) {return CharacterPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#countDistinct(double[],boolean) */
    public static  long countDistinct( double[] values, boolean countNull ) {return DoublePrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#countDistinct(float[],boolean) */
    public static  long countDistinct( float[] values, boolean countNull ) {return FloatPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#countDistinct(int[],boolean) */
    public static  long countDistinct( int[] values, boolean countNull ) {return IntegerPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#countDistinct(long[],boolean) */
    public static  long countDistinct( long[] values, boolean countNull ) {return LongPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#countDistinct(short[],boolean) */
    public static  long countDistinct( short[] values, boolean countNull ) {return ShortPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbByteArray,boolean) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbByteArray values, boolean countNull ) {return BytePrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbCharArray,boolean) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbCharArray values, boolean countNull ) {return CharacterPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbDoubleArray,boolean) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbDoubleArray values, boolean countNull ) {return DoublePrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbFloatArray,boolean) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbFloatArray values, boolean countNull ) {return FloatPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbIntArray,boolean) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbIntArray values, boolean countNull ) {return IntegerPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbLongArray,boolean) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbLongArray values, boolean countNull ) {return LongPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbShortArray,boolean) */
    public static  long countDistinct( io.deephaven.db.tables.dbarrays.DbShortArray values, boolean countNull ) {return ShortPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#countDistinct(io.deephaven.db.tables.dbarrays.DbArray,boolean) */
    public static <T extends java.lang.Comparable<? super T>> long countDistinct( io.deephaven.db.tables.dbarrays.DbArray<T> values, boolean countNull ) {return ObjectPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#countNeg(byte[]) */
    public static  int countNeg( byte[] values ) {return ByteNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#countNeg(double[]) */
    public static  int countNeg( double[] values ) {return DoubleNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#countNeg(float[]) */
    public static  int countNeg( float[] values ) {return FloatNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#countNeg(int[]) */
    public static  int countNeg( int[] values ) {return IntegerNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#countNeg(long[]) */
    public static  int countNeg( long[] values ) {return LongNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#countNeg(java.lang.Byte[]) */
    public static  int countNeg( java.lang.Byte[] values ) {return ByteNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#countNeg(java.lang.Double[]) */
    public static  int countNeg( java.lang.Double[] values ) {return DoubleNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#countNeg(java.lang.Float[]) */
    public static  int countNeg( java.lang.Float[] values ) {return FloatNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#countNeg(java.lang.Integer[]) */
    public static  int countNeg( java.lang.Integer[] values ) {return IntegerNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#countNeg(java.lang.Long[]) */
    public static  int countNeg( java.lang.Long[] values ) {return LongNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#countNeg(java.lang.Short[]) */
    public static  int countNeg( java.lang.Short[] values ) {return ShortNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#countNeg(short[]) */
    public static  int countNeg( short[] values ) {return ShortNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#countNeg(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  int countNeg( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#countNeg(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  int countNeg( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#countNeg(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  int countNeg( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#countNeg(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int countNeg( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#countNeg(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  int countNeg( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#countNeg(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  int countNeg( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#countPos(byte[]) */
    public static  int countPos( byte[] values ) {return ByteNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#countPos(double[]) */
    public static  int countPos( double[] values ) {return DoubleNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#countPos(float[]) */
    public static  int countPos( float[] values ) {return FloatNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#countPos(int[]) */
    public static  int countPos( int[] values ) {return IntegerNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#countPos(long[]) */
    public static  int countPos( long[] values ) {return LongNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#countPos(java.lang.Byte[]) */
    public static  int countPos( java.lang.Byte[] values ) {return ByteNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#countPos(java.lang.Double[]) */
    public static  int countPos( java.lang.Double[] values ) {return DoubleNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#countPos(java.lang.Float[]) */
    public static  int countPos( java.lang.Float[] values ) {return FloatNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#countPos(java.lang.Integer[]) */
    public static  int countPos( java.lang.Integer[] values ) {return IntegerNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#countPos(java.lang.Long[]) */
    public static  int countPos( java.lang.Long[] values ) {return LongNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#countPos(java.lang.Short[]) */
    public static  int countPos( java.lang.Short[] values ) {return ShortNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#countPos(short[]) */
    public static  int countPos( short[] values ) {return ShortNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#countPos(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  int countPos( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#countPos(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  int countPos( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#countPos(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  int countPos( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#countPos(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int countPos( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#countPos(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  int countPos( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#countPos(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  int countPos( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.countPos( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#countZero(byte[]) */
    public static  int countZero( byte[] values ) {return ByteNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#countZero(double[]) */
    public static  int countZero( double[] values ) {return DoubleNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#countZero(float[]) */
    public static  int countZero( float[] values ) {return FloatNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#countZero(int[]) */
    public static  int countZero( int[] values ) {return IntegerNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#countZero(long[]) */
    public static  int countZero( long[] values ) {return LongNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#countZero(java.lang.Byte[]) */
    public static  int countZero( java.lang.Byte[] values ) {return ByteNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#countZero(java.lang.Double[]) */
    public static  int countZero( java.lang.Double[] values ) {return DoubleNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#countZero(java.lang.Float[]) */
    public static  int countZero( java.lang.Float[] values ) {return FloatNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#countZero(java.lang.Integer[]) */
    public static  int countZero( java.lang.Integer[] values ) {return IntegerNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#countZero(java.lang.Long[]) */
    public static  int countZero( java.lang.Long[] values ) {return LongNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#countZero(java.lang.Short[]) */
    public static  int countZero( java.lang.Short[] values ) {return ShortNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#countZero(short[]) */
    public static  int countZero( short[] values ) {return ShortNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#countZero(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  int countZero( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#countZero(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  int countZero( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#countZero(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  int countZero( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#countZero(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int countZero( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#countZero(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  int countZero( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#countZero(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  int countZero( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.countZero( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cov(byte[],byte[]) */
    public static  double cov( byte[] values0, byte[] values1 ) {return ByteNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cov(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double cov( byte[] values0, io.deephaven.db.tables.dbarrays.DbByteArray values1 ) {return ByteNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cov(double[],double[]) */
    public static  double cov( double[] values0, double[] values1 ) {return DoubleNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cov(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double cov( double[] values0, io.deephaven.db.tables.dbarrays.DbDoubleArray values1 ) {return DoubleNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cov(float[],float[]) */
    public static  double cov( float[] values0, float[] values1 ) {return FloatNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cov(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double cov( float[] values0, io.deephaven.db.tables.dbarrays.DbFloatArray values1 ) {return FloatNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cov(int[],int[]) */
    public static  double cov( int[] values0, int[] values1 ) {return IntegerNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cov(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double cov( int[] values0, io.deephaven.db.tables.dbarrays.DbIntArray values1 ) {return IntegerNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cov(long[],long[]) */
    public static  double cov( long[] values0, long[] values1 ) {return LongNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cov(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double cov( long[] values0, io.deephaven.db.tables.dbarrays.DbLongArray values1 ) {return LongNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cov(short[],short[]) */
    public static  double cov( short[] values0, short[] values1 ) {return ShortNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cov(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double cov( short[] values0, io.deephaven.db.tables.dbarrays.DbShortArray values1 ) {return ShortNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbByteArray values0, byte[] values1 ) {return ByteNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbByteArray values0, io.deephaven.db.tables.dbarrays.DbByteArray values1 ) {return ByteNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbDoubleArray values0, double[] values1 ) {return DoubleNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbDoubleArray values0, io.deephaven.db.tables.dbarrays.DbDoubleArray values1 ) {return DoubleNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbFloatArray values0, float[] values1 ) {return FloatNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbFloatArray values0, io.deephaven.db.tables.dbarrays.DbFloatArray values1 ) {return FloatNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbIntArray values0, int[] values1 ) {return IntegerNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbIntArray values0, io.deephaven.db.tables.dbarrays.DbIntArray values1 ) {return IntegerNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbLongArray values0, long[] values1 ) {return LongNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbLongArray values0, io.deephaven.db.tables.dbarrays.DbLongArray values1 ) {return LongNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbShortArray values0, short[] values1 ) {return ShortNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cov(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double cov( io.deephaven.db.tables.dbarrays.DbShortArray values0, io.deephaven.db.tables.dbarrays.DbShortArray values1 ) {return ShortNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cumprod(byte[]) */
    public static  byte[] cumprod( byte[] values ) {return ByteNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cumprod(double[]) */
    public static  double[] cumprod( double[] values ) {return DoubleNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cumprod(float[]) */
    public static  float[] cumprod( float[] values ) {return FloatNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cumprod(int[]) */
    public static  int[] cumprod( int[] values ) {return IntegerNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cumprod(long[]) */
    public static  long[] cumprod( long[] values ) {return LongNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cumprod(java.lang.Byte[]) */
    public static  byte[] cumprod( java.lang.Byte[] values ) {return ByteNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cumprod(java.lang.Double[]) */
    public static  double[] cumprod( java.lang.Double[] values ) {return DoubleNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cumprod(java.lang.Float[]) */
    public static  float[] cumprod( java.lang.Float[] values ) {return FloatNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cumprod(java.lang.Integer[]) */
    public static  int[] cumprod( java.lang.Integer[] values ) {return IntegerNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cumprod(java.lang.Long[]) */
    public static  long[] cumprod( java.lang.Long[] values ) {return LongNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cumprod(java.lang.Short[]) */
    public static  short[] cumprod( java.lang.Short[] values ) {return ShortNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cumprod(short[]) */
    public static  short[] cumprod( short[] values ) {return ShortNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cumprod(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte[] cumprod( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cumprod(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double[] cumprod( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cumprod(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float[] cumprod( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cumprod(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int[] cumprod( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cumprod(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long[] cumprod( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cumprod(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short[] cumprod( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cumsum(byte[]) */
    public static  byte[] cumsum( byte[] values ) {return ByteNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cumsum(double[]) */
    public static  double[] cumsum( double[] values ) {return DoubleNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cumsum(float[]) */
    public static  float[] cumsum( float[] values ) {return FloatNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cumsum(int[]) */
    public static  int[] cumsum( int[] values ) {return IntegerNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cumsum(long[]) */
    public static  long[] cumsum( long[] values ) {return LongNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cumsum(java.lang.Byte[]) */
    public static  byte[] cumsum( java.lang.Byte[] values ) {return ByteNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cumsum(java.lang.Double[]) */
    public static  double[] cumsum( java.lang.Double[] values ) {return DoubleNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cumsum(java.lang.Float[]) */
    public static  float[] cumsum( java.lang.Float[] values ) {return FloatNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cumsum(java.lang.Integer[]) */
    public static  int[] cumsum( java.lang.Integer[] values ) {return IntegerNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cumsum(java.lang.Long[]) */
    public static  long[] cumsum( java.lang.Long[] values ) {return LongNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cumsum(java.lang.Short[]) */
    public static  short[] cumsum( java.lang.Short[] values ) {return ShortNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cumsum(short[]) */
    public static  short[] cumsum( short[] values ) {return ShortNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#cumsum(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte[] cumsum( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#cumsum(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double[] cumsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#cumsum(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float[] cumsum( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#cumsum(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int[] cumsum( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#cumsum(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long[] cumsum( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#cumsum(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short[] cumsum( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#distinct(byte[]) */
    public static  byte[] distinct( byte[] values ) {return BytePrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#distinct(char[]) */
    public static  char[] distinct( char[] values ) {return CharacterPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#distinct(double[]) */
    public static  double[] distinct( double[] values ) {return DoublePrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#distinct(float[]) */
    public static  float[] distinct( float[] values ) {return FloatPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#distinct(int[]) */
    public static  int[] distinct( int[] values ) {return IntegerPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#distinct(long[]) */
    public static  long[] distinct( long[] values ) {return LongPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#distinct(short[]) */
    public static  short[] distinct( short[] values ) {return ShortPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#distinct(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  io.deephaven.db.tables.dbarrays.DbByteArray distinct( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return BytePrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbCharArray) */
    public static  io.deephaven.db.tables.dbarrays.DbCharArray distinct( io.deephaven.db.tables.dbarrays.DbCharArray values ) {return CharacterPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#distinct(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  io.deephaven.db.tables.dbarrays.DbDoubleArray distinct( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoublePrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  io.deephaven.db.tables.dbarrays.DbFloatArray distinct( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  io.deephaven.db.tables.dbarrays.DbIntArray distinct( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  io.deephaven.db.tables.dbarrays.DbLongArray distinct( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  io.deephaven.db.tables.dbarrays.DbShortArray distinct( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T extends java.lang.Comparable<? super T>> io.deephaven.db.tables.dbarrays.DbArray<T> distinct( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.distinct( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#distinct(byte[],boolean,boolean) */
    public static  byte[] distinct( byte[] values, boolean includeNull, boolean sort ) {return BytePrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#distinct(char[],boolean,boolean) */
    public static  char[] distinct( char[] values, boolean includeNull, boolean sort ) {return CharacterPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#distinct(double[],boolean,boolean) */
    public static  double[] distinct( double[] values, boolean includeNull, boolean sort ) {return DoublePrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#distinct(float[],boolean,boolean) */
    public static  float[] distinct( float[] values, boolean includeNull, boolean sort ) {return FloatPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#distinct(int[],boolean,boolean) */
    public static  int[] distinct( int[] values, boolean includeNull, boolean sort ) {return IntegerPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#distinct(long[],boolean,boolean) */
    public static  long[] distinct( long[] values, boolean includeNull, boolean sort ) {return LongPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#distinct(short[],boolean,boolean) */
    public static  short[] distinct( short[] values, boolean includeNull, boolean sort ) {return ShortPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#distinct(io.deephaven.db.tables.dbarrays.DbByteArray,boolean,boolean) */
    public static  io.deephaven.db.tables.dbarrays.DbByteArray distinct( io.deephaven.db.tables.dbarrays.DbByteArray values, boolean includeNull, boolean sort ) {return BytePrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbCharArray,boolean,boolean) */
    public static  io.deephaven.db.tables.dbarrays.DbCharArray distinct( io.deephaven.db.tables.dbarrays.DbCharArray values, boolean includeNull, boolean sort ) {return CharacterPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#distinct(io.deephaven.db.tables.dbarrays.DbDoubleArray,boolean,boolean) */
    public static  io.deephaven.db.tables.dbarrays.DbDoubleArray distinct( io.deephaven.db.tables.dbarrays.DbDoubleArray values, boolean includeNull, boolean sort ) {return DoublePrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbFloatArray,boolean,boolean) */
    public static  io.deephaven.db.tables.dbarrays.DbFloatArray distinct( io.deephaven.db.tables.dbarrays.DbFloatArray values, boolean includeNull, boolean sort ) {return FloatPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbIntArray,boolean,boolean) */
    public static  io.deephaven.db.tables.dbarrays.DbIntArray distinct( io.deephaven.db.tables.dbarrays.DbIntArray values, boolean includeNull, boolean sort ) {return IntegerPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbLongArray,boolean,boolean) */
    public static  io.deephaven.db.tables.dbarrays.DbLongArray distinct( io.deephaven.db.tables.dbarrays.DbLongArray values, boolean includeNull, boolean sort ) {return LongPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbShortArray,boolean,boolean) */
    public static  io.deephaven.db.tables.dbarrays.DbShortArray distinct( io.deephaven.db.tables.dbarrays.DbShortArray values, boolean includeNull, boolean sort ) {return ShortPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#distinct(io.deephaven.db.tables.dbarrays.DbArray,boolean,boolean) */
    public static <T extends java.lang.Comparable<? super T>> io.deephaven.db.tables.dbarrays.DbArray<T> distinct( io.deephaven.db.tables.dbarrays.DbArray<T> values, boolean includeNull, boolean sort ) {return ObjectPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#enlist(byte[]) */
    public static  byte[] enlist( byte[] values ) {return BytePrimitives.enlist( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#enlist(char[]) */
    public static  char[] enlist( char[] values ) {return CharacterPrimitives.enlist( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#enlist(double[]) */
    public static  double[] enlist( double[] values ) {return DoublePrimitives.enlist( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#enlist(float[]) */
    public static  float[] enlist( float[] values ) {return FloatPrimitives.enlist( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#enlist(int[]) */
    public static  int[] enlist( int[] values ) {return IntegerPrimitives.enlist( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#enlist(long[]) */
    public static  long[] enlist( long[] values ) {return LongPrimitives.enlist( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#enlist(short[]) */
    public static  short[] enlist( short[] values ) {return ShortPrimitives.enlist( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#exp(byte) */
    public static  double exp( byte value ) {return ByteNumericPrimitives.exp( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#exp(double) */
    public static  double exp( double value ) {return DoubleNumericPrimitives.exp( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#exp(float) */
    public static  double exp( float value ) {return FloatNumericPrimitives.exp( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#exp(int) */
    public static  double exp( int value ) {return IntegerNumericPrimitives.exp( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#exp(long) */
    public static  double exp( long value ) {return LongNumericPrimitives.exp( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#exp(short) */
    public static  double exp( short value ) {return ShortNumericPrimitives.exp( value );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#first(byte[]) */
    public static  byte first( byte[] values ) {return BytePrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#first(char[]) */
    public static  char first( char[] values ) {return CharacterPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#first(double[]) */
    public static  double first( double[] values ) {return DoublePrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#first(float[]) */
    public static  float first( float[] values ) {return FloatPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#first(int[]) */
    public static  int first( int[] values ) {return IntegerPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#first(long[]) */
    public static  long first( long[] values ) {return LongPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#first(java.lang.Boolean[]) */
    public static  java.lang.Boolean first( java.lang.Boolean[] values ) {return BooleanPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#first(short[]) */
    public static  short first( short[] values ) {return ShortPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#first(boolean[]) */
    public static  java.lang.Boolean first( boolean[] values ) {return BooleanPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#first(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte first( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return BytePrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#first(io.deephaven.db.tables.dbarrays.DbCharArray) */
    public static  char first( io.deephaven.db.tables.dbarrays.DbCharArray values ) {return CharacterPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#first(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double first( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoublePrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#first(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float first( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#first(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int first( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#first(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long first( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#first(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short first( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#first(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T> T first( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.first( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#firstIndexOf(byte[],byte) */
    public static  int firstIndexOf( byte[] values, byte val ) {return ByteNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#firstIndexOf(double[],double) */
    public static  int firstIndexOf( double[] values, double val ) {return DoubleNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#firstIndexOf(float[],float) */
    public static  int firstIndexOf( float[] values, float val ) {return FloatNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#firstIndexOf(int[],int) */
    public static  int firstIndexOf( int[] values, int val ) {return IntegerNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#firstIndexOf(long[],long) */
    public static  int firstIndexOf( long[] values, long val ) {return LongNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#firstIndexOf(short[],short) */
    public static  int firstIndexOf( short[] values, short val ) {return ShortNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#firstIndexOf(io.deephaven.db.tables.dbarrays.DbByteArray,byte) */
    public static  int firstIndexOf( io.deephaven.db.tables.dbarrays.DbByteArray values, byte val ) {return ByteNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#firstIndexOf(io.deephaven.db.tables.dbarrays.DbDoubleArray,double) */
    public static  int firstIndexOf( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double val ) {return DoubleNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#firstIndexOf(io.deephaven.db.tables.dbarrays.DbFloatArray,float) */
    public static  int firstIndexOf( io.deephaven.db.tables.dbarrays.DbFloatArray values, float val ) {return FloatNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#firstIndexOf(io.deephaven.db.tables.dbarrays.DbIntArray,int) */
    public static  int firstIndexOf( io.deephaven.db.tables.dbarrays.DbIntArray values, int val ) {return IntegerNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#firstIndexOf(io.deephaven.db.tables.dbarrays.DbLongArray,long) */
    public static  int firstIndexOf( io.deephaven.db.tables.dbarrays.DbLongArray values, long val ) {return LongNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#firstIndexOf(io.deephaven.db.tables.dbarrays.DbShortArray,short) */
    public static  int firstIndexOf( io.deephaven.db.tables.dbarrays.DbShortArray values, short val ) {return ShortNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#floor(byte) */
    public static  double floor( byte value ) {return ByteNumericPrimitives.floor( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#floor(double) */
    public static  double floor( double value ) {return DoubleNumericPrimitives.floor( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#floor(float) */
    public static  double floor( float value ) {return FloatNumericPrimitives.floor( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#floor(int) */
    public static  double floor( int value ) {return IntegerNumericPrimitives.floor( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#floor(long) */
    public static  double floor( long value ) {return LongNumericPrimitives.floor( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#floor(short) */
    public static  double floor( short value ) {return ShortNumericPrimitives.floor( value );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#in(T,T[]) */
    public static <T> boolean in( T testedValue, T[] possibleValues ) {return ObjectPrimitives.in( testedValue, possibleValues );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#in(byte,byte[]) */
    public static  boolean in( byte testedValues, byte[] possibleValues ) {return BytePrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#in(char,char[]) */
    public static  boolean in( char testedValues, char[] possibleValues ) {return CharacterPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#in(double,double[]) */
    public static  boolean in( double testedValues, double[] possibleValues ) {return DoublePrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#in(float,float[]) */
    public static  boolean in( float testedValues, float[] possibleValues ) {return FloatPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#in(int,int[]) */
    public static  boolean in( int testedValues, int[] possibleValues ) {return IntegerPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#in(long,long[]) */
    public static  boolean in( long testedValues, long[] possibleValues ) {return LongPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#in(short,short[]) */
    public static  boolean in( short testedValues, short[] possibleValues ) {return ShortPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#inRange(byte,byte,byte) */
    public static  boolean inRange( byte testedValue, byte lowInclusiveValue, byte highInclusiveValue ) {return BytePrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#inRange(char,char,char) */
    public static  boolean inRange( char testedValue, char lowInclusiveValue, char highInclusiveValue ) {return CharacterPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#inRange(double,double,double) */
    public static  boolean inRange( double testedValue, double lowInclusiveValue, double highInclusiveValue ) {return DoublePrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#inRange(float,float,float) */
    public static  boolean inRange( float testedValue, float lowInclusiveValue, float highInclusiveValue ) {return FloatPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#inRange(int,int,int) */
    public static  boolean inRange( int testedValue, int lowInclusiveValue, int highInclusiveValue ) {return IntegerPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#inRange(java.lang.Comparable,java.lang.Comparable,java.lang.Comparable) */
    public static  boolean inRange( java.lang.Comparable testedValue, java.lang.Comparable lowInclusiveValue, java.lang.Comparable highInclusiveValue ) {return ObjectPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#inRange(long,long,long) */
    public static  boolean inRange( long testedValue, long lowInclusiveValue, long highInclusiveValue ) {return LongPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#inRange(short,short,short) */
    public static  boolean inRange( short testedValue, short lowInclusiveValue, short highInclusiveValue ) {return ShortPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#indexOfMax(byte[]) */
    public static  int indexOfMax( byte[] values ) {return ByteNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#indexOfMax(double[]) */
    public static  int indexOfMax( double[] values ) {return DoubleNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#indexOfMax(float[]) */
    public static  int indexOfMax( float[] values ) {return FloatNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#indexOfMax(int[]) */
    public static  int indexOfMax( int[] values ) {return IntegerNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#indexOfMax(long[]) */
    public static  int indexOfMax( long[] values ) {return LongNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#indexOfMax(java.lang.Byte[]) */
    public static  int indexOfMax( java.lang.Byte[] values ) {return ByteNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#indexOfMax(java.lang.Double[]) */
    public static  int indexOfMax( java.lang.Double[] values ) {return DoubleNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#indexOfMax(java.lang.Float[]) */
    public static  int indexOfMax( java.lang.Float[] values ) {return FloatNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#indexOfMax(java.lang.Integer[]) */
    public static  int indexOfMax( java.lang.Integer[] values ) {return IntegerNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#indexOfMax(java.lang.Long[]) */
    public static  int indexOfMax( java.lang.Long[] values ) {return LongNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#indexOfMax(java.lang.Short[]) */
    public static  int indexOfMax( java.lang.Short[] values ) {return ShortNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#indexOfMax(short[]) */
    public static  int indexOfMax( short[] values ) {return ShortNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#indexOfMax(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  int indexOfMax( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#indexOfMax(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  int indexOfMax( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#indexOfMax(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  int indexOfMax( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#indexOfMax(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int indexOfMax( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#indexOfMax(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  int indexOfMax( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#indexOfMax(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  int indexOfMax( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#indexOfMin(byte[]) */
    public static  int indexOfMin( byte[] values ) {return ByteNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#indexOfMin(double[]) */
    public static  int indexOfMin( double[] values ) {return DoubleNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#indexOfMin(float[]) */
    public static  int indexOfMin( float[] values ) {return FloatNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#indexOfMin(int[]) */
    public static  int indexOfMin( int[] values ) {return IntegerNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#indexOfMin(long[]) */
    public static  int indexOfMin( long[] values ) {return LongNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#indexOfMin(java.lang.Byte[]) */
    public static  int indexOfMin( java.lang.Byte[] values ) {return ByteNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#indexOfMin(java.lang.Double[]) */
    public static  int indexOfMin( java.lang.Double[] values ) {return DoubleNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#indexOfMin(java.lang.Float[]) */
    public static  int indexOfMin( java.lang.Float[] values ) {return FloatNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#indexOfMin(java.lang.Integer[]) */
    public static  int indexOfMin( java.lang.Integer[] values ) {return IntegerNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#indexOfMin(java.lang.Long[]) */
    public static  int indexOfMin( java.lang.Long[] values ) {return LongNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#indexOfMin(java.lang.Short[]) */
    public static  int indexOfMin( java.lang.Short[] values ) {return ShortNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#indexOfMin(short[]) */
    public static  int indexOfMin( short[] values ) {return ShortNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#indexOfMin(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  int indexOfMin( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#indexOfMin(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  int indexOfMin( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#indexOfMin(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  int indexOfMin( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#indexOfMin(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int indexOfMin( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#indexOfMin(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  int indexOfMin( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#indexOfMin(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  int indexOfMin( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.libs.primitives.Casting#intToDouble(int[]) */
    public static  double[] intToDouble( int[] values ) {return Casting.intToDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#intToDouble(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  io.deephaven.db.tables.dbarrays.DbDoubleArray intToDouble( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return Casting.intToDouble( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#isDBNull(T) */
    public static <T> boolean isDBNull( T value ) {return ObjectPrimitives.isDBNull( value );}
    /** @see io.deephaven.libs.primitives.DoubleFpPrimitives#isInf(double) */
    public static  boolean isInf( double value ) {return DoubleFpPrimitives.isInf( value );}
    /** @see io.deephaven.libs.primitives.FloatFpPrimitives#isInf(float) */
    public static  boolean isInf( float value ) {return FloatFpPrimitives.isInf( value );}
    /** @see io.deephaven.libs.primitives.DoubleFpPrimitives#isNaN(double) */
    public static  boolean isNaN( double value ) {return DoubleFpPrimitives.isNaN( value );}
    /** @see io.deephaven.libs.primitives.FloatFpPrimitives#isNaN(float) */
    public static  boolean isNaN( float value ) {return FloatFpPrimitives.isNaN( value );}
    /** @see io.deephaven.libs.primitives.DoubleFpPrimitives#isNormal(double) */
    public static  boolean isNormal( double value ) {return DoubleFpPrimitives.isNormal( value );}
    /** @see io.deephaven.libs.primitives.FloatFpPrimitives#isNormal(float) */
    public static  boolean isNormal( float value ) {return FloatFpPrimitives.isNormal( value );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#isNull(T) */
    public static <T> boolean isNull( T value ) {return ObjectPrimitives.isNull( value );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#isNull(byte) */
    public static  boolean isNull( byte value ) {return BytePrimitives.isNull( value );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#isNull(char) */
    public static  boolean isNull( char value ) {return CharacterPrimitives.isNull( value );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#isNull(java.lang.Boolean) */
    public static  boolean isNull( java.lang.Boolean value ) {return BooleanPrimitives.isNull( value );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#isNull(double) */
    public static  boolean isNull( double value ) {return DoublePrimitives.isNull( value );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#isNull(float) */
    public static  boolean isNull( float value ) {return FloatPrimitives.isNull( value );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#isNull(int) */
    public static  boolean isNull( int value ) {return IntegerPrimitives.isNull( value );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#isNull(long) */
    public static  boolean isNull( long value ) {return LongPrimitives.isNull( value );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#isNull(short) */
    public static  boolean isNull( short value ) {return ShortPrimitives.isNull( value );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#last(byte[]) */
    public static  byte last( byte[] values ) {return BytePrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#last(char[]) */
    public static  char last( char[] values ) {return CharacterPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#last(double[]) */
    public static  double last( double[] values ) {return DoublePrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#last(float[]) */
    public static  float last( float[] values ) {return FloatPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#last(int[]) */
    public static  int last( int[] values ) {return IntegerPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#last(long[]) */
    public static  long last( long[] values ) {return LongPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#last(java.lang.Boolean[]) */
    public static  java.lang.Boolean last( java.lang.Boolean[] values ) {return BooleanPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#last(short[]) */
    public static  short last( short[] values ) {return ShortPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#last(boolean[]) */
    public static  java.lang.Boolean last( boolean[] values ) {return BooleanPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#last(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte last( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return BytePrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#last(io.deephaven.db.tables.dbarrays.DbCharArray) */
    public static  char last( io.deephaven.db.tables.dbarrays.DbCharArray values ) {return CharacterPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#last(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double last( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoublePrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#last(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float last( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#last(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int last( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#last(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long last( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#last(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short last( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#last(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T> T last( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.last( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#log(byte) */
    public static  double log( byte value ) {return ByteNumericPrimitives.log( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#log(double) */
    public static  double log( double value ) {return DoubleNumericPrimitives.log( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#log(float) */
    public static  double log( float value ) {return FloatNumericPrimitives.log( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#log(int) */
    public static  double log( int value ) {return IntegerNumericPrimitives.log( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#log(long) */
    public static  double log( long value ) {return LongNumericPrimitives.log( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#log(short) */
    public static  double log( short value ) {return ShortNumericPrimitives.log( value );}
    /** @see io.deephaven.libs.primitives.Casting#longToDouble(long[]) */
    public static  double[] longToDouble( long[] values ) {return Casting.longToDouble( values );}
    /** @see io.deephaven.libs.primitives.Casting#longToDouble(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  io.deephaven.db.tables.dbarrays.DbDoubleArray longToDouble( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return Casting.longToDouble( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#lowerBin(byte,byte) */
    public static  byte lowerBin( byte value, byte interval ) {return ByteNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#lowerBin(double,double) */
    public static  double lowerBin( double value, double interval ) {return DoubleNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#lowerBin(float,float) */
    public static  float lowerBin( float value, float interval ) {return FloatNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#lowerBin(int,int) */
    public static  int lowerBin( int value, int interval ) {return IntegerNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#lowerBin(long,long) */
    public static  long lowerBin( long value, long interval ) {return LongNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#lowerBin(short,short) */
    public static  short lowerBin( short value, short interval ) {return ShortNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#lowerBin(byte,byte,byte) */
    public static  byte lowerBin( byte value, byte interval, byte offset ) {return ByteNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#lowerBin(double,double,double) */
    public static  double lowerBin( double value, double interval, double offset ) {return DoubleNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#lowerBin(float,float,float) */
    public static  float lowerBin( float value, float interval, float offset ) {return FloatNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#lowerBin(int,int,int) */
    public static  int lowerBin( int value, int interval, int offset ) {return IntegerNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#lowerBin(long,long,long) */
    public static  long lowerBin( long value, long interval, long offset ) {return LongNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#lowerBin(short,short,short) */
    public static  short lowerBin( short value, short interval, short offset ) {return ShortNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#max(NUM[]) */
    public static <NUM extends java.lang.Number> NUM max( NUM[] values ) {return ObjectPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#max(T[]) */
    public static <T> T max( T[] values ) {return ObjectPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#max(byte[]) */
    public static  byte max( byte[] values ) {return ByteNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#max(double[]) */
    public static  double max( double[] values ) {return DoubleNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#max(float[]) */
    public static  float max( float[] values ) {return FloatNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#max(int[]) */
    public static  int max( int[] values ) {return IntegerNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#max(long[]) */
    public static  long max( long[] values ) {return LongNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#max(java.lang.Byte[]) */
    public static  byte max( java.lang.Byte[] values ) {return ByteNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#max(java.lang.Double[]) */
    public static  double max( java.lang.Double[] values ) {return DoubleNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#max(java.lang.Float[]) */
    public static  float max( java.lang.Float[] values ) {return FloatNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#max(java.lang.Integer[]) */
    public static  int max( java.lang.Integer[] values ) {return IntegerNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#max(java.lang.Long[]) */
    public static  long max( java.lang.Long[] values ) {return LongNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#max(java.lang.Short[]) */
    public static  short max( java.lang.Short[] values ) {return ShortNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#max(short[]) */
    public static  short max( short[] values ) {return ShortNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#max(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte max( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#max(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double max( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#max(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float max( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#max(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int max( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#max(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long max( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#max(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short max( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#max(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T extends java.lang.Comparable> T max( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.max( values );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(byte,byte) */
    public static  byte max( byte v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(byte,double) */
    public static  double max( byte v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(byte,float) */
    public static  float max( byte v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(byte,int) */
    public static  int max( byte v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(byte,long) */
    public static  long max( byte v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(byte,short) */
    public static  short max( byte v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(double,byte) */
    public static  double max( double v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(double,double) */
    public static  double max( double v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(double,float) */
    public static  double max( double v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(double,int) */
    public static  double max( double v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(double,long) */
    public static  double max( double v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(double,short) */
    public static  double max( double v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(float,byte) */
    public static  float max( float v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(float,double) */
    public static  double max( float v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(float,float) */
    public static  float max( float v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(float,int) */
    public static  double max( float v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(float,long) */
    public static  double max( float v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(float,short) */
    public static  float max( float v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(int,byte) */
    public static  int max( int v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(int,double) */
    public static  double max( int v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(int,float) */
    public static  double max( int v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(int,int) */
    public static  int max( int v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(int,long) */
    public static  long max( int v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(int,short) */
    public static  int max( int v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(long,byte) */
    public static  long max( long v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(long,double) */
    public static  double max( long v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(long,float) */
    public static  double max( long v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(long,int) */
    public static  long max( long v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(long,long) */
    public static  long max( long v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(long,short) */
    public static  long max( long v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(short,byte) */
    public static  short max( short v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(short,double) */
    public static  double max( short v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(short,float) */
    public static  float max( short v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(short,int) */
    public static  int max( short v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(short,long) */
    public static  long max( short v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#max(short,short) */
    public static  short max( short v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#median(byte[]) */
    public static  double median( byte[] values ) {return ByteNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#median(double[]) */
    public static  double median( double[] values ) {return DoubleNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#median(float[]) */
    public static  double median( float[] values ) {return FloatNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#median(int[]) */
    public static  double median( int[] values ) {return IntegerNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#median(long[]) */
    public static  double median( long[] values ) {return LongNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#median(java.lang.Byte[]) */
    public static  double median( java.lang.Byte[] values ) {return ByteNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#median(java.lang.Double[]) */
    public static  double median( java.lang.Double[] values ) {return DoubleNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#median(java.lang.Float[]) */
    public static  double median( java.lang.Float[] values ) {return FloatNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#median(java.lang.Integer[]) */
    public static  double median( java.lang.Integer[] values ) {return IntegerNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#median(java.lang.Long[]) */
    public static  double median( java.lang.Long[] values ) {return LongNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#median(java.lang.Short[]) */
    public static  double median( java.lang.Short[] values ) {return ShortNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#median(short[]) */
    public static  double median( short[] values ) {return ShortNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#median(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double median( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#median(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double median( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#median(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double median( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#median(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double median( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#median(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double median( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#median(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double median( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.median( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#min(NUM[]) */
    public static <NUM extends java.lang.Number> NUM min( NUM[] values ) {return ObjectPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#min(T[]) */
    public static <T> T min( T[] values ) {return ObjectPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#min(byte[]) */
    public static  byte min( byte[] values ) {return ByteNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#min(double[]) */
    public static  double min( double[] values ) {return DoubleNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#min(float[]) */
    public static  float min( float[] values ) {return FloatNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#min(int[]) */
    public static  int min( int[] values ) {return IntegerNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#min(long[]) */
    public static  long min( long[] values ) {return LongNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#min(java.lang.Byte[]) */
    public static  byte min( java.lang.Byte[] values ) {return ByteNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#min(java.lang.Double[]) */
    public static  double min( java.lang.Double[] values ) {return DoubleNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#min(java.lang.Float[]) */
    public static  float min( java.lang.Float[] values ) {return FloatNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#min(java.lang.Integer[]) */
    public static  int min( java.lang.Integer[] values ) {return IntegerNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#min(java.lang.Long[]) */
    public static  long min( java.lang.Long[] values ) {return LongNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#min(java.lang.Short[]) */
    public static  short min( java.lang.Short[] values ) {return ShortNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#min(short[]) */
    public static  short min( short[] values ) {return ShortNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#min(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte min( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#min(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double min( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#min(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float min( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#min(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int min( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#min(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long min( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#min(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short min( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#min(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T extends java.lang.Comparable> T min( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.min( values );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(byte,byte) */
    public static  byte min( byte v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(byte,double) */
    public static  double min( byte v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(byte,float) */
    public static  float min( byte v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(byte,int) */
    public static  int min( byte v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(byte,long) */
    public static  long min( byte v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(byte,short) */
    public static  short min( byte v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(double,byte) */
    public static  double min( double v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(double,double) */
    public static  double min( double v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(double,float) */
    public static  double min( double v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(double,int) */
    public static  double min( double v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(double,long) */
    public static  double min( double v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(double,short) */
    public static  double min( double v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(float,byte) */
    public static  float min( float v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(float,double) */
    public static  double min( float v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(float,float) */
    public static  float min( float v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(float,int) */
    public static  double min( float v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(float,long) */
    public static  double min( float v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(float,short) */
    public static  float min( float v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(int,byte) */
    public static  int min( int v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(int,double) */
    public static  double min( int v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(int,float) */
    public static  double min( int v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(int,int) */
    public static  int min( int v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(int,long) */
    public static  long min( int v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(int,short) */
    public static  int min( int v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(long,byte) */
    public static  long min( long v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(long,double) */
    public static  double min( long v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(long,float) */
    public static  double min( long v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(long,int) */
    public static  long min( long v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(long,long) */
    public static  long min( long v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(long,short) */
    public static  long min( long v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(short,byte) */
    public static  short min( short v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(short,double) */
    public static  double min( short v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(short,float) */
    public static  float min( short v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(short,int) */
    public static  int min( short v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(short,long) */
    public static  long min( short v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.ComparePrimitives#min(short,short) */
    public static  short min( short v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#not(java.lang.Boolean[]) */
    public static  java.lang.Boolean[] not( java.lang.Boolean[] values ) {return BooleanPrimitives.not( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#not(boolean[]) */
    public static  java.lang.Boolean[] not( boolean[] values ) {return BooleanPrimitives.not( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#nth(int,byte[]) */
    public static  byte nth( int index, byte[] values ) {return BytePrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#nth(int,char[]) */
    public static  char nth( int index, char[] values ) {return CharacterPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#nth(int,double[]) */
    public static  double nth( int index, double[] values ) {return DoublePrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#nth(int,float[]) */
    public static  float nth( int index, float[] values ) {return FloatPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#nth(int,int[]) */
    public static  int nth( int index, int[] values ) {return IntegerPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#nth(int,long[]) */
    public static  long nth( int index, long[] values ) {return LongPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#nth(int,java.lang.Boolean[]) */
    public static  java.lang.Boolean nth( int index, java.lang.Boolean[] values ) {return BooleanPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#nth(int,short[]) */
    public static  short nth( int index, short[] values ) {return ShortPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#nth(int,io.deephaven.db.tables.dbarrays.DbBooleanArray) */
    public static  java.lang.Boolean nth( int index, io.deephaven.db.tables.dbarrays.DbBooleanArray values ) {return BooleanPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#nth(int,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte nth( int index, io.deephaven.db.tables.dbarrays.DbByteArray values ) {return BytePrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#nth(int,io.deephaven.db.tables.dbarrays.DbCharArray) */
    public static  char nth( int index, io.deephaven.db.tables.dbarrays.DbCharArray values ) {return CharacterPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#nth(int,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double nth( int index, io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoublePrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#nth(int,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float nth( int index, io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#nth(int,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int nth( int index, io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#nth(int,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long nth( int index, io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#nth(int,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short nth( int index, io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#nth(int,io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T> T nth( int index, io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.nth( index, values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#nullToValue(T,T) */
    public static <T> T nullToValue( T value, T defaultValue ) {return ObjectPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#nullToValue(byte,byte) */
    public static  byte nullToValue( byte value, byte defaultValue ) {return BytePrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#nullToValue(char,char) */
    public static  char nullToValue( char value, char defaultValue ) {return CharacterPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#nullToValue(byte[],byte) */
    public static  byte[] nullToValue( byte[] values, byte defaultValue ) {return BytePrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#nullToValue(char[],char) */
    public static  char[] nullToValue( char[] values, char defaultValue ) {return CharacterPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#nullToValue(double[],double) */
    public static  double[] nullToValue( double[] values, double defaultValue ) {return DoublePrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#nullToValue(float[],float) */
    public static  float[] nullToValue( float[] values, float defaultValue ) {return FloatPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#nullToValue(int[],int) */
    public static  int[] nullToValue( int[] values, int defaultValue ) {return IntegerPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#nullToValue(long[],long) */
    public static  long[] nullToValue( long[] values, long defaultValue ) {return LongPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#nullToValue(short[],short) */
    public static  short[] nullToValue( short[] values, short defaultValue ) {return ShortPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#nullToValue(java.lang.Boolean,boolean) */
    public static  java.lang.Boolean nullToValue( java.lang.Boolean value, boolean defaultValue ) {return BooleanPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#nullToValue(double,double) */
    public static  double nullToValue( double value, double defaultValue ) {return DoublePrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#nullToValue(float,float) */
    public static  float nullToValue( float value, float defaultValue ) {return FloatPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#nullToValue(int,int) */
    public static  int nullToValue( int value, int defaultValue ) {return IntegerPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#nullToValue(io.deephaven.db.tables.dbarrays.DbBooleanArray,boolean) */
    public static  java.lang.Boolean[] nullToValue( io.deephaven.db.tables.dbarrays.DbBooleanArray values, boolean defaultValue ) {return BooleanPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#nullToValue(io.deephaven.db.tables.dbarrays.DbByteArray,byte) */
    public static  byte[] nullToValue( io.deephaven.db.tables.dbarrays.DbByteArray values, byte defaultValue ) {return BytePrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#nullToValue(io.deephaven.db.tables.dbarrays.DbCharArray,char) */
    public static  char[] nullToValue( io.deephaven.db.tables.dbarrays.DbCharArray values, char defaultValue ) {return CharacterPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#nullToValue(io.deephaven.db.tables.dbarrays.DbDoubleArray,double) */
    public static  double[] nullToValue( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double defaultValue ) {return DoublePrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#nullToValue(io.deephaven.db.tables.dbarrays.DbFloatArray,float) */
    public static  float[] nullToValue( io.deephaven.db.tables.dbarrays.DbFloatArray values, float defaultValue ) {return FloatPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#nullToValue(io.deephaven.db.tables.dbarrays.DbIntArray,int) */
    public static  int[] nullToValue( io.deephaven.db.tables.dbarrays.DbIntArray values, int defaultValue ) {return IntegerPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#nullToValue(io.deephaven.db.tables.dbarrays.DbLongArray,long) */
    public static  long[] nullToValue( io.deephaven.db.tables.dbarrays.DbLongArray values, long defaultValue ) {return LongPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#nullToValue(io.deephaven.db.tables.dbarrays.DbShortArray,short) */
    public static  short[] nullToValue( io.deephaven.db.tables.dbarrays.DbShortArray values, short defaultValue ) {return ShortPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#nullToValue(io.deephaven.db.tables.dbarrays.DbArray,T) */
    public static <T> T[] nullToValue( io.deephaven.db.tables.dbarrays.DbArray<T> values, T defaultValue ) {return ObjectPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#nullToValue(long,long) */
    public static  long nullToValue( long value, long defaultValue ) {return LongPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#nullToValue(short,short) */
    public static  short nullToValue( short value, short defaultValue ) {return ShortPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#or(java.lang.Boolean[]) */
    public static  java.lang.Boolean or( java.lang.Boolean[] values ) {return BooleanPrimitives.or( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#or(boolean[]) */
    public static  java.lang.Boolean or( boolean[] values ) {return BooleanPrimitives.or( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#or(java.lang.Boolean[],java.lang.Boolean) */
    public static  java.lang.Boolean or( java.lang.Boolean[] values, java.lang.Boolean nullValue ) {return BooleanPrimitives.or( values, nullValue );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#percentile(double[],double) */
    public static  double percentile( double[] values, double percentile ) {return DoubleNumericPrimitives.percentile( values, percentile );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#percentile(float[],double) */
    public static  double percentile( float[] values, double percentile ) {return FloatNumericPrimitives.percentile( values, percentile );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#percentile(double,byte[]) */
    public static  double percentile( double percentile, byte[] values ) {return ByteNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#percentile(double,int[]) */
    public static  double percentile( double percentile, int[] values ) {return IntegerNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#percentile(double,long[]) */
    public static  double percentile( double percentile, long[] values ) {return LongNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#percentile(double,short[]) */
    public static  double percentile( double percentile, short[] values ) {return ShortNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#percentile(double,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double percentile( double percentile, io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#percentile(double,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double percentile( double percentile, io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#percentile(double,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double percentile( double percentile, io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#percentile(double,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double percentile( double percentile, io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#percentile(io.deephaven.db.tables.dbarrays.DbDoubleArray,double) */
    public static  double percentile( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double percentile ) {return DoubleNumericPrimitives.percentile( values, percentile );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#percentile(io.deephaven.db.tables.dbarrays.DbFloatArray,double) */
    public static  double percentile( io.deephaven.db.tables.dbarrays.DbFloatArray values, double percentile ) {return FloatNumericPrimitives.percentile( values, percentile );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#pow(byte,byte) */
    public static  double pow( byte a, byte b ) {return ByteNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#pow(double,double) */
    public static  double pow( double a, double b ) {return DoubleNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#pow(float,float) */
    public static  double pow( float a, float b ) {return FloatNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#pow(int,int) */
    public static  double pow( int a, int b ) {return IntegerNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#pow(long,long) */
    public static  double pow( long a, long b ) {return LongNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#pow(short,short) */
    public static  double pow( short a, short b ) {return ShortNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#product(byte[]) */
    public static  byte product( byte[] values ) {return ByteNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#product(double[]) */
    public static  double product( double[] values ) {return DoubleNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#product(float[]) */
    public static  float product( float[] values ) {return FloatNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#product(int[]) */
    public static  int product( int[] values ) {return IntegerNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#product(long[]) */
    public static  long product( long[] values ) {return LongNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#product(short[]) */
    public static  short product( short[] values ) {return ShortNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#product(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte product( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#product(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double product( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#product(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float product( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#product(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int product( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#product(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long product( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#product(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short product( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.product( values );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#random() */
    public static  double random( ) {return SpecialPrimitives.random( );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomBool() */
    public static  boolean randomBool( ) {return SpecialPrimitives.randomBool( );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomBool(int) */
    public static  boolean[] randomBool( int size ) {return SpecialPrimitives.randomBool( size );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomDouble(double,double) */
    public static  double randomDouble( double min, double max ) {return SpecialPrimitives.randomDouble( min, max );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomDouble(double,double,int) */
    public static  double[] randomDouble( double min, double max, int size ) {return SpecialPrimitives.randomDouble( min, max, size );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomFloat(float,float) */
    public static  float randomFloat( float min, float max ) {return SpecialPrimitives.randomFloat( min, max );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomFloat(float,float,int) */
    public static  float[] randomFloat( float min, float max, int size ) {return SpecialPrimitives.randomFloat( min, max, size );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomGaussian(double,double) */
    public static  double randomGaussian( double mean, double std ) {return SpecialPrimitives.randomGaussian( mean, std );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomGaussian(double,double,int) */
    public static  double[] randomGaussian( double mean, double std, int size ) {return SpecialPrimitives.randomGaussian( mean, std, size );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomInt(int,int) */
    public static  int randomInt( int min, int max ) {return SpecialPrimitives.randomInt( min, max );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomInt(int,int,int) */
    public static  int[] randomInt( int min, int max, int size ) {return SpecialPrimitives.randomInt( min, max, size );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomLong(long,long) */
    public static  long randomLong( long min, long max ) {return SpecialPrimitives.randomLong( min, max );}
    /** @see io.deephaven.libs.primitives.SpecialPrimitives#randomLong(long,long,int) */
    public static  long[] randomLong( long min, long max, int size ) {return SpecialPrimitives.randomLong( min, max, size );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#rawBinSearchIndex(byte[],byte,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( byte[] values, byte key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ByteNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#rawBinSearchIndex(double[],double,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( double[] values, double key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return DoubleNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#rawBinSearchIndex(float[],float,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( float[] values, float key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return FloatNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#rawBinSearchIndex(int[],int,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( int[] values, int key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return IntegerNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#rawBinSearchIndex(long[],long,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( long[] values, long key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return LongNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#rawBinSearchIndex(short[],short,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( short[] values, short key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ShortNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#rawBinSearchIndex(io.deephaven.db.tables.dbarrays.DbByteArray,byte,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.db.tables.dbarrays.DbByteArray values, byte key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ByteNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#rawBinSearchIndex(io.deephaven.db.tables.dbarrays.DbDoubleArray,double,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return DoubleNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#rawBinSearchIndex(io.deephaven.db.tables.dbarrays.DbFloatArray,float,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.db.tables.dbarrays.DbFloatArray values, float key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return FloatNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#rawBinSearchIndex(io.deephaven.db.tables.dbarrays.DbIntArray,int,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.db.tables.dbarrays.DbIntArray values, int key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return IntegerNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#rawBinSearchIndex(io.deephaven.db.tables.dbarrays.DbLongArray,long,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.db.tables.dbarrays.DbLongArray values, long key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return LongNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#rawBinSearchIndex(io.deephaven.db.tables.dbarrays.DbShortArray,short,io.deephaven.libs.primitives.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.db.tables.dbarrays.DbShortArray values, short key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ShortNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#rawBinSearchIndex(io.deephaven.db.tables.dbarrays.DbArray,T,io.deephaven.libs.primitives.BinSearch) */
    public static <T extends java.lang.Comparable<? super T>> int rawBinSearchIndex( io.deephaven.db.tables.dbarrays.DbArray<T> values, T key, io.deephaven.libs.primitives.BinSearch choiceWhenEquals ) {return ObjectPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#repeat(byte,int) */
    public static  byte[] repeat( byte value, int size ) {return BytePrimitives.repeat( value, size );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#repeat(char,int) */
    public static  char[] repeat( char value, int size ) {return CharacterPrimitives.repeat( value, size );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#repeat(double,int) */
    public static  double[] repeat( double value, int size ) {return DoublePrimitives.repeat( value, size );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#repeat(float,int) */
    public static  float[] repeat( float value, int size ) {return FloatPrimitives.repeat( value, size );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#repeat(int,int) */
    public static  int[] repeat( int value, int size ) {return IntegerPrimitives.repeat( value, size );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#repeat(long,int) */
    public static  long[] repeat( long value, int size ) {return LongPrimitives.repeat( value, size );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#repeat(short,int) */
    public static  short[] repeat( short value, int size ) {return ShortPrimitives.repeat( value, size );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#reverse(byte[]) */
    public static  byte[] reverse( byte[] values ) {return BytePrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#reverse(char[]) */
    public static  char[] reverse( char[] values ) {return CharacterPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#reverse(double[]) */
    public static  double[] reverse( double[] values ) {return DoublePrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#reverse(float[]) */
    public static  float[] reverse( float[] values ) {return FloatPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#reverse(int[]) */
    public static  int[] reverse( int[] values ) {return IntegerPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#reverse(long[]) */
    public static  long[] reverse( long[] values ) {return LongPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#reverse(short[]) */
    public static  short[] reverse( short[] values ) {return ShortPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#reverse(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte[] reverse( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return BytePrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#reverse(io.deephaven.db.tables.dbarrays.DbCharArray) */
    public static  char[] reverse( io.deephaven.db.tables.dbarrays.DbCharArray values ) {return CharacterPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#reverse(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double[] reverse( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoublePrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#reverse(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float[] reverse( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#reverse(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int[] reverse( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#reverse(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long[] reverse( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#reverse(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short[] reverse( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortPrimitives.reverse( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#rint(byte) */
    public static  double rint( byte value ) {return ByteNumericPrimitives.rint( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#rint(double) */
    public static  double rint( double value ) {return DoubleNumericPrimitives.rint( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#rint(float) */
    public static  double rint( float value ) {return FloatNumericPrimitives.rint( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#rint(int) */
    public static  double rint( int value ) {return IntegerNumericPrimitives.rint( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#rint(long) */
    public static  double rint( long value ) {return LongNumericPrimitives.rint( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#rint(short) */
    public static  double rint( short value ) {return ShortNumericPrimitives.rint( value );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#round(byte) */
    public static  long round( byte value ) {return ByteNumericPrimitives.round( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#round(double) */
    public static  long round( double value ) {return DoubleNumericPrimitives.round( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#round(float) */
    public static  long round( float value ) {return FloatNumericPrimitives.round( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#round(int) */
    public static  long round( int value ) {return IntegerNumericPrimitives.round( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#round(long) */
    public static  long round( long value ) {return LongNumericPrimitives.round( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#round(short) */
    public static  long round( short value ) {return ShortNumericPrimitives.round( value );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sequence(byte,byte,byte) */
    public static  byte[] sequence( byte start, byte end, byte step ) {return ByteNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sequence(double,double,double) */
    public static  double[] sequence( double start, double end, double step ) {return DoubleNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sequence(float,float,float) */
    public static  float[] sequence( float start, float end, float step ) {return FloatNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sequence(int,int,int) */
    public static  int[] sequence( int start, int end, int step ) {return IntegerNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sequence(long,long,long) */
    public static  long[] sequence( long start, long end, long step ) {return LongNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sequence(short,short,short) */
    public static  short[] sequence( short start, short end, short step ) {return ShortNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#signum(byte) */
    public static  byte signum( byte value ) {return ByteNumericPrimitives.signum( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#signum(double) */
    public static  double signum( double value ) {return DoubleNumericPrimitives.signum( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#signum(float) */
    public static  float signum( float value ) {return FloatNumericPrimitives.signum( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#signum(int) */
    public static  int signum( int value ) {return IntegerNumericPrimitives.signum( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#signum(long) */
    public static  long signum( long value ) {return LongNumericPrimitives.signum( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#signum(short) */
    public static  short signum( short value ) {return ShortNumericPrimitives.signum( value );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sin(byte) */
    public static  double sin( byte value ) {return ByteNumericPrimitives.sin( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sin(double) */
    public static  double sin( double value ) {return DoubleNumericPrimitives.sin( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sin(float) */
    public static  double sin( float value ) {return FloatNumericPrimitives.sin( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sin(int) */
    public static  double sin( int value ) {return IntegerNumericPrimitives.sin( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sin(long) */
    public static  double sin( long value ) {return LongNumericPrimitives.sin( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sin(short) */
    public static  double sin( short value ) {return ShortNumericPrimitives.sin( value );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#sort(NUM[]) */
    public static <NUM extends java.lang.Number> NUM[] sort( NUM[] values ) {return ObjectPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#sort(T[]) */
    public static <T> T[] sort( T[] values ) {return ObjectPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sort(byte[]) */
    public static  byte[] sort( byte[] values ) {return ByteNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sort(double[]) */
    public static  double[] sort( double[] values ) {return DoubleNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sort(float[]) */
    public static  float[] sort( float[] values ) {return FloatNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sort(int[]) */
    public static  int[] sort( int[] values ) {return IntegerNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sort(long[]) */
    public static  long[] sort( long[] values ) {return LongNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sort(java.lang.Byte[]) */
    public static  byte[] sort( java.lang.Byte[] values ) {return ByteNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sort(java.lang.Double[]) */
    public static  double[] sort( java.lang.Double[] values ) {return DoubleNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sort(java.lang.Float[]) */
    public static  float[] sort( java.lang.Float[] values ) {return FloatNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sort(java.lang.Integer[]) */
    public static  int[] sort( java.lang.Integer[] values ) {return IntegerNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sort(java.lang.Long[]) */
    public static  long[] sort( java.lang.Long[] values ) {return LongNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sort(java.lang.Short[]) */
    public static  short[] sort( java.lang.Short[] values ) {return ShortNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sort(short[]) */
    public static  short[] sort( short[] values ) {return ShortNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sort(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  io.deephaven.db.tables.dbarrays.DbByteArray sort( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sort(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  io.deephaven.db.tables.dbarrays.DbDoubleArray sort( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sort(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  io.deephaven.db.tables.dbarrays.DbFloatArray sort( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sort(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  io.deephaven.db.tables.dbarrays.DbIntArray sort( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sort(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  io.deephaven.db.tables.dbarrays.DbLongArray sort( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sort(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  io.deephaven.db.tables.dbarrays.DbShortArray sort( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#sort(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T extends java.lang.Comparable<? super T>> io.deephaven.db.tables.dbarrays.DbArray<T> sort( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.sort( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#sortDescending(NUM[]) */
    public static <NUM extends java.lang.Number> NUM[] sortDescending( NUM[] values ) {return ObjectPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#sortDescending(T[]) */
    public static <T> T[] sortDescending( T[] values ) {return ObjectPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sortDescending(byte[]) */
    public static  byte[] sortDescending( byte[] values ) {return ByteNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sortDescending(double[]) */
    public static  double[] sortDescending( double[] values ) {return DoubleNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sortDescending(float[]) */
    public static  float[] sortDescending( float[] values ) {return FloatNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sortDescending(int[]) */
    public static  int[] sortDescending( int[] values ) {return IntegerNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sortDescending(long[]) */
    public static  long[] sortDescending( long[] values ) {return LongNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sortDescending(java.lang.Byte[]) */
    public static  byte[] sortDescending( java.lang.Byte[] values ) {return ByteNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sortDescending(java.lang.Double[]) */
    public static  double[] sortDescending( java.lang.Double[] values ) {return DoubleNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sortDescending(java.lang.Float[]) */
    public static  float[] sortDescending( java.lang.Float[] values ) {return FloatNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sortDescending(java.lang.Integer[]) */
    public static  int[] sortDescending( java.lang.Integer[] values ) {return IntegerNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sortDescending(java.lang.Long[]) */
    public static  long[] sortDescending( java.lang.Long[] values ) {return LongNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sortDescending(java.lang.Short[]) */
    public static  short[] sortDescending( java.lang.Short[] values ) {return ShortNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sortDescending(short[]) */
    public static  short[] sortDescending( short[] values ) {return ShortNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sortDescending(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  io.deephaven.db.tables.dbarrays.DbByteArray sortDescending( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sortDescending(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  io.deephaven.db.tables.dbarrays.DbDoubleArray sortDescending( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sortDescending(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  io.deephaven.db.tables.dbarrays.DbFloatArray sortDescending( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sortDescending(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  io.deephaven.db.tables.dbarrays.DbIntArray sortDescending( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sortDescending(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  io.deephaven.db.tables.dbarrays.DbLongArray sortDescending( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sortDescending(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  io.deephaven.db.tables.dbarrays.DbShortArray sortDescending( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#sortDescending(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T extends java.lang.Comparable<? super T>> io.deephaven.db.tables.dbarrays.DbArray<T> sortDescending( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.sortDescending( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sqrt(byte) */
    public static  double sqrt( byte value ) {return ByteNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sqrt(double) */
    public static  double sqrt( double value ) {return DoubleNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sqrt(float) */
    public static  double sqrt( float value ) {return FloatNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sqrt(int) */
    public static  double sqrt( int value ) {return IntegerNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sqrt(long) */
    public static  double sqrt( long value ) {return LongNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sqrt(short) */
    public static  double sqrt( short value ) {return ShortNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#std(byte[]) */
    public static  double std( byte[] values ) {return ByteNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#std(double[]) */
    public static  double std( double[] values ) {return DoubleNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#std(float[]) */
    public static  double std( float[] values ) {return FloatNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#std(int[]) */
    public static  double std( int[] values ) {return IntegerNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#std(long[]) */
    public static  double std( long[] values ) {return LongNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#std(java.lang.Byte[]) */
    public static  double std( java.lang.Byte[] values ) {return ByteNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#std(java.lang.Double[]) */
    public static  double std( java.lang.Double[] values ) {return DoubleNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#std(java.lang.Float[]) */
    public static  double std( java.lang.Float[] values ) {return FloatNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#std(java.lang.Integer[]) */
    public static  double std( java.lang.Integer[] values ) {return IntegerNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#std(java.lang.Long[]) */
    public static  double std( java.lang.Long[] values ) {return LongNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#std(java.lang.Short[]) */
    public static  double std( java.lang.Short[] values ) {return ShortNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#std(short[]) */
    public static  double std( short[] values ) {return ShortNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#std(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double std( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#std(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double std( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#std(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double std( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#std(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double std( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#std(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double std( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#std(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double std( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.std( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#ste(byte[]) */
    public static  double ste( byte[] values ) {return ByteNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#ste(double[]) */
    public static  double ste( double[] values ) {return DoubleNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#ste(float[]) */
    public static  double ste( float[] values ) {return FloatNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#ste(int[]) */
    public static  double ste( int[] values ) {return IntegerNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#ste(long[]) */
    public static  double ste( long[] values ) {return LongNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#ste(java.lang.Byte[]) */
    public static  double ste( java.lang.Byte[] values ) {return ByteNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#ste(java.lang.Double[]) */
    public static  double ste( java.lang.Double[] values ) {return DoubleNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#ste(java.lang.Float[]) */
    public static  double ste( java.lang.Float[] values ) {return FloatNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#ste(java.lang.Integer[]) */
    public static  double ste( java.lang.Integer[] values ) {return IntegerNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#ste(java.lang.Long[]) */
    public static  double ste( java.lang.Long[] values ) {return LongNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#ste(java.lang.Short[]) */
    public static  double ste( java.lang.Short[] values ) {return ShortNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#ste(short[]) */
    public static  double ste( short[] values ) {return ShortNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#ste(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double ste( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#ste(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double ste( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#ste(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double ste( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#ste(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double ste( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#ste(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double ste( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#ste(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double ste( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.ste( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sum(byte[]) */
    public static  byte sum( byte[] values ) {return ByteNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sum(double[]) */
    public static  double sum( double[] values ) {return DoubleNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sum(float[]) */
    public static  float sum( float[] values ) {return FloatNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sum(int[]) */
    public static  int sum( int[] values ) {return IntegerNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sum(long[]) */
    public static  long sum( long[] values ) {return LongNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#sum(java.lang.Boolean[]) */
    public static  java.lang.Boolean sum( java.lang.Boolean[] values ) {return BooleanPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sum(short[]) */
    public static  short sum( short[] values ) {return ShortNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#sum(boolean[]) */
    public static  java.lang.Boolean sum( boolean[] values ) {return BooleanPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sum(byte[][]) */
    public static  byte[] sum( byte[][] values ) {return ByteNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sum(double[][]) */
    public static  double[] sum( double[][] values ) {return DoubleNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sum(float[][]) */
    public static  float[] sum( float[][] values ) {return FloatNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sum(int[][]) */
    public static  int[] sum( int[][] values ) {return IntegerNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sum(long[][]) */
    public static  long[] sum( long[][] values ) {return LongNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sum(short[][]) */
    public static  short[] sum( short[][] values ) {return ShortNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#sum(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte sum( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#sum(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double sum( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#sum(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float sum( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#sum(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int sum( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#sum(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long sum( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#sum(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short sum( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.sum( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#tan(byte) */
    public static  double tan( byte value ) {return ByteNumericPrimitives.tan( value );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#tan(double) */
    public static  double tan( double value ) {return DoubleNumericPrimitives.tan( value );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#tan(float) */
    public static  double tan( float value ) {return FloatNumericPrimitives.tan( value );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#tan(int) */
    public static  double tan( int value ) {return IntegerNumericPrimitives.tan( value );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#tan(long) */
    public static  double tan( long value ) {return LongNumericPrimitives.tan( value );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#tan(short) */
    public static  double tan( short value ) {return ShortNumericPrimitives.tan( value );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#tstat(byte[]) */
    public static  double tstat( byte[] values ) {return ByteNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#tstat(double[]) */
    public static  double tstat( double[] values ) {return DoubleNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#tstat(float[]) */
    public static  double tstat( float[] values ) {return FloatNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#tstat(int[]) */
    public static  double tstat( int[] values ) {return IntegerNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#tstat(long[]) */
    public static  double tstat( long[] values ) {return LongNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#tstat(java.lang.Byte[]) */
    public static  double tstat( java.lang.Byte[] values ) {return ByteNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#tstat(java.lang.Double[]) */
    public static  double tstat( java.lang.Double[] values ) {return DoubleNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#tstat(java.lang.Float[]) */
    public static  double tstat( java.lang.Float[] values ) {return FloatNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#tstat(java.lang.Integer[]) */
    public static  double tstat( java.lang.Integer[] values ) {return IntegerNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#tstat(java.lang.Long[]) */
    public static  double tstat( java.lang.Long[] values ) {return LongNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#tstat(java.lang.Short[]) */
    public static  double tstat( java.lang.Short[] values ) {return ShortNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#tstat(short[]) */
    public static  double tstat( short[] values ) {return ShortNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#tstat(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double tstat( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#tstat(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double tstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#tstat(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double tstat( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#tstat(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double tstat( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#tstat(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double tstat( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#tstat(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double tstat( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.tstat( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#unbox(java.lang.Byte[]) */
    public static  byte[] unbox( java.lang.Byte[] values ) {return BytePrimitives.unbox( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#unbox(java.lang.Character[]) */
    public static  char[] unbox( java.lang.Character[] values ) {return CharacterPrimitives.unbox( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#unbox(java.lang.Double[]) */
    public static  double[] unbox( java.lang.Double[] values ) {return DoublePrimitives.unbox( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#unbox(java.lang.Float[]) */
    public static  float[] unbox( java.lang.Float[] values ) {return FloatPrimitives.unbox( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#unbox(java.lang.Integer[]) */
    public static  int[] unbox( java.lang.Integer[] values ) {return IntegerPrimitives.unbox( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#unbox(java.lang.Long[]) */
    public static  long[] unbox( java.lang.Long[] values ) {return LongPrimitives.unbox( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#unbox(java.lang.Short[]) */
    public static  short[] unbox( java.lang.Short[] values ) {return ShortPrimitives.unbox( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#uniqueValue(io.deephaven.db.tables.dbarrays.DbByteArray,boolean) */
    public static  byte uniqueValue( io.deephaven.db.tables.dbarrays.DbByteArray arr, boolean countNull ) {return BytePrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#uniqueValue(io.deephaven.db.tables.dbarrays.DbCharArray,boolean) */
    public static  char uniqueValue( io.deephaven.db.tables.dbarrays.DbCharArray arr, boolean countNull ) {return CharacterPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#uniqueValue(io.deephaven.db.tables.dbarrays.DbDoubleArray,boolean) */
    public static  double uniqueValue( io.deephaven.db.tables.dbarrays.DbDoubleArray arr, boolean countNull ) {return DoublePrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#uniqueValue(io.deephaven.db.tables.dbarrays.DbFloatArray,boolean) */
    public static  float uniqueValue( io.deephaven.db.tables.dbarrays.DbFloatArray arr, boolean countNull ) {return FloatPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#uniqueValue(io.deephaven.db.tables.dbarrays.DbIntArray,boolean) */
    public static  int uniqueValue( io.deephaven.db.tables.dbarrays.DbIntArray arr, boolean countNull ) {return IntegerPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#uniqueValue(io.deephaven.db.tables.dbarrays.DbLongArray,boolean) */
    public static  long uniqueValue( io.deephaven.db.tables.dbarrays.DbLongArray arr, boolean countNull ) {return LongPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#uniqueValue(io.deephaven.db.tables.dbarrays.DbShortArray,boolean) */
    public static  short uniqueValue( io.deephaven.db.tables.dbarrays.DbShortArray arr, boolean countNull ) {return ShortPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#upperBin(byte,byte) */
    public static  byte upperBin( byte value, byte interval ) {return ByteNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#upperBin(double,double) */
    public static  double upperBin( double value, double interval ) {return DoubleNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#upperBin(float,float) */
    public static  float upperBin( float value, float interval ) {return FloatNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#upperBin(int,int) */
    public static  int upperBin( int value, int interval ) {return IntegerNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#upperBin(long,long) */
    public static  long upperBin( long value, long interval ) {return LongNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#upperBin(short,short) */
    public static  short upperBin( short value, short interval ) {return ShortNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#upperBin(byte,byte,byte) */
    public static  byte upperBin( byte value, byte interval, byte offset ) {return ByteNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#upperBin(double,double,double) */
    public static  double upperBin( double value, double interval, double offset ) {return DoubleNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#upperBin(float,float,float) */
    public static  float upperBin( float value, float interval, float offset ) {return FloatNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#upperBin(int,int,int) */
    public static  int upperBin( int value, int interval, int offset ) {return IntegerNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#upperBin(long,long,long) */
    public static  long upperBin( long value, long interval, long offset ) {return LongNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#upperBin(short,short,short) */
    public static  short upperBin( short value, short interval, short offset ) {return ShortNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#var(byte[]) */
    public static  double var( byte[] values ) {return ByteNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#var(double[]) */
    public static  double var( double[] values ) {return DoubleNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#var(float[]) */
    public static  double var( float[] values ) {return FloatNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#var(int[]) */
    public static  double var( int[] values ) {return IntegerNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#var(long[]) */
    public static  double var( long[] values ) {return LongNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#var(java.lang.Byte[]) */
    public static  double var( java.lang.Byte[] values ) {return ByteNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#var(java.lang.Double[]) */
    public static  double var( java.lang.Double[] values ) {return DoubleNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#var(java.lang.Float[]) */
    public static  double var( java.lang.Float[] values ) {return FloatNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#var(java.lang.Integer[]) */
    public static  double var( java.lang.Integer[] values ) {return IntegerNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#var(java.lang.Long[]) */
    public static  double var( java.lang.Long[] values ) {return LongNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#var(java.lang.Short[]) */
    public static  double var( java.lang.Short[] values ) {return ShortNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#var(short[]) */
    public static  double var( short[] values ) {return ShortNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#var(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double var( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return ByteNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#var(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double var( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoubleNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#var(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double var( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#var(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double var( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#var(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double var( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#var(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double var( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortNumericPrimitives.var( values );}
    /** @see io.deephaven.libs.primitives.BooleanPrimitives#vec(io.deephaven.db.tables.dbarrays.DbBooleanArray) */
    public static  java.lang.Boolean[] vec( io.deephaven.db.tables.dbarrays.DbBooleanArray values ) {return BooleanPrimitives.vec( values );}
    /** @see io.deephaven.libs.primitives.BytePrimitives#vec(io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  byte[] vec( io.deephaven.db.tables.dbarrays.DbByteArray values ) {return BytePrimitives.vec( values );}
    /** @see io.deephaven.libs.primitives.CharacterPrimitives#vec(io.deephaven.db.tables.dbarrays.DbCharArray) */
    public static  char[] vec( io.deephaven.db.tables.dbarrays.DbCharArray values ) {return CharacterPrimitives.vec( values );}
    /** @see io.deephaven.libs.primitives.DoublePrimitives#vec(io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double[] vec( io.deephaven.db.tables.dbarrays.DbDoubleArray values ) {return DoublePrimitives.vec( values );}
    /** @see io.deephaven.libs.primitives.FloatPrimitives#vec(io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  float[] vec( io.deephaven.db.tables.dbarrays.DbFloatArray values ) {return FloatPrimitives.vec( values );}
    /** @see io.deephaven.libs.primitives.IntegerPrimitives#vec(io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  int[] vec( io.deephaven.db.tables.dbarrays.DbIntArray values ) {return IntegerPrimitives.vec( values );}
    /** @see io.deephaven.libs.primitives.LongPrimitives#vec(io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  long[] vec( io.deephaven.db.tables.dbarrays.DbLongArray values ) {return LongPrimitives.vec( values );}
    /** @see io.deephaven.libs.primitives.ShortPrimitives#vec(io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  short[] vec( io.deephaven.db.tables.dbarrays.DbShortArray values ) {return ShortPrimitives.vec( values );}
    /** @see io.deephaven.libs.primitives.ObjectPrimitives#vec(io.deephaven.db.tables.dbarrays.DbArray) */
    public static <T> T[] vec( io.deephaven.db.tables.dbarrays.DbArray<T> values ) {return ObjectPrimitives.vec( values );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],byte[]) */
    public static  double wavg( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],double[]) */
    public static  double wavg( byte[] values, double[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],float[]) */
    public static  double wavg( byte[] values, float[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],int[]) */
    public static  double wavg( byte[] values, int[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],long[]) */
    public static  double wavg( byte[] values, long[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wavg( byte[] values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( byte[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( byte[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( byte[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(byte[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( byte[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],double[]) */
    public static  double wavg( double[] values, double[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],float[]) */
    public static  double wavg( double[] values, float[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],int[]) */
    public static  double wavg( double[] values, int[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],long[]) */
    public static  double wavg( double[] values, long[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],short[]) */
    public static  double wavg( double[] values, short[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( double[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( double[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( double[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( double[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(double[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( double[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],double[]) */
    public static  double wavg( float[] values, double[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],float[]) */
    public static  double wavg( float[] values, float[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],int[]) */
    public static  double wavg( float[] values, int[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],long[]) */
    public static  double wavg( float[] values, long[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],short[]) */
    public static  double wavg( float[] values, short[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( float[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( float[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( float[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( float[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(float[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( float[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],double[]) */
    public static  double wavg( int[] values, double[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],float[]) */
    public static  double wavg( int[] values, float[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],int[]) */
    public static  double wavg( int[] values, int[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],long[]) */
    public static  double wavg( int[] values, long[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],short[]) */
    public static  double wavg( int[] values, short[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( int[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( int[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( int[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( int[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(int[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( int[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],double[]) */
    public static  double wavg( long[] values, double[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],float[]) */
    public static  double wavg( long[] values, float[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],int[]) */
    public static  double wavg( long[] values, int[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],long[]) */
    public static  double wavg( long[] values, long[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],short[]) */
    public static  double wavg( long[] values, short[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( long[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( long[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( long[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( long[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(long[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( long[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],double[]) */
    public static  double wavg( short[] values, double[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],float[]) */
    public static  double wavg( short[] values, float[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],int[]) */
    public static  double wavg( short[] values, int[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],long[]) */
    public static  double wavg( short[] values, long[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],short[]) */
    public static  double wavg( short[] values, short[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( short[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( short[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( short[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( short[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( short[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, byte[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,double[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, double[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,float[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, float[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,int[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, int[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,long[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, long[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,float[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, float[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,int[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, int[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,long[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, long[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,short[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, short[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,double[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, double[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, float[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,int[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, int[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,long[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, long[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,short[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, short[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,double[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, double[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,float[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, float[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, int[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,long[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, long[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,short[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, short[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,double[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, double[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,float[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, float[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,int[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, int[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, long[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,short[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, short[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,double[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, double[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,float[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, float[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,int[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, int[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,long[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, long[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, short[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wavg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wavg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],byte[]) */
    public static  double weightedAvg( byte[] values, byte[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],double[]) */
    public static  double weightedAvg( byte[] values, double[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],float[]) */
    public static  double weightedAvg( byte[] values, float[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],int[]) */
    public static  double weightedAvg( byte[] values, int[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],long[]) */
    public static  double weightedAvg( byte[] values, long[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double weightedAvg( byte[] values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( byte[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( byte[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( byte[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( byte[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],double[]) */
    public static  double weightedAvg( double[] values, double[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],float[]) */
    public static  double weightedAvg( double[] values, float[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],int[]) */
    public static  double weightedAvg( double[] values, int[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],long[]) */
    public static  double weightedAvg( double[] values, long[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],short[]) */
    public static  double weightedAvg( double[] values, short[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( double[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( double[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( double[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( double[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( double[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],double[]) */
    public static  double weightedAvg( float[] values, double[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],float[]) */
    public static  double weightedAvg( float[] values, float[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],int[]) */
    public static  double weightedAvg( float[] values, int[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],long[]) */
    public static  double weightedAvg( float[] values, long[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],short[]) */
    public static  double weightedAvg( float[] values, short[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( float[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( float[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( float[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( float[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( float[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],double[]) */
    public static  double weightedAvg( int[] values, double[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],float[]) */
    public static  double weightedAvg( int[] values, float[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],int[]) */
    public static  double weightedAvg( int[] values, int[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],long[]) */
    public static  double weightedAvg( int[] values, long[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],short[]) */
    public static  double weightedAvg( int[] values, short[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( int[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( int[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( int[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( int[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( int[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],double[]) */
    public static  double weightedAvg( long[] values, double[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],float[]) */
    public static  double weightedAvg( long[] values, float[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],int[]) */
    public static  double weightedAvg( long[] values, int[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],long[]) */
    public static  double weightedAvg( long[] values, long[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],short[]) */
    public static  double weightedAvg( long[] values, short[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( long[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( long[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( long[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( long[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(long[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( long[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],double[]) */
    public static  double weightedAvg( short[] values, double[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],float[]) */
    public static  double weightedAvg( short[] values, float[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],int[]) */
    public static  double weightedAvg( short[] values, int[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],long[]) */
    public static  double weightedAvg( short[] values, long[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],short[]) */
    public static  double weightedAvg( short[] values, short[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( short[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( short[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( short[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( short[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( short[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, byte[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,double[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, double[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,float[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, float[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,int[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, int[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,long[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, long[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,float[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, float[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,int[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, int[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,long[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, long[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,short[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, short[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,double[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, double[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, float[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,int[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, int[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,long[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, long[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,short[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, short[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,double[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, double[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,float[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, float[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, int[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,long[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, long[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,short[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, short[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,double[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, double[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,float[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, float[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,int[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, int[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, long[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,short[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, short[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,double[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, double[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,float[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, float[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,int[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, int[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,long[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, long[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, short[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedAvg(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedAvg( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],byte[]) */
    public static  double weightedSum( byte[] values, byte[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],double[]) */
    public static  double weightedSum( byte[] values, double[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],float[]) */
    public static  double weightedSum( byte[] values, float[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],int[]) */
    public static  double weightedSum( byte[] values, int[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],long[]) */
    public static  double weightedSum( byte[] values, long[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double weightedSum( byte[] values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( byte[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( byte[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( byte[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( byte[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],double[]) */
    public static  double weightedSum( double[] values, double[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],float[]) */
    public static  double weightedSum( double[] values, float[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],int[]) */
    public static  double weightedSum( double[] values, int[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],long[]) */
    public static  double weightedSum( double[] values, long[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],short[]) */
    public static  double weightedSum( double[] values, short[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( double[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( double[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( double[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( double[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( double[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],double[]) */
    public static  double weightedSum( float[] values, double[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],float[]) */
    public static  double weightedSum( float[] values, float[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],int[]) */
    public static  double weightedSum( float[] values, int[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],long[]) */
    public static  double weightedSum( float[] values, long[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],short[]) */
    public static  double weightedSum( float[] values, short[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( float[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( float[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( float[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( float[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(float[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( float[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],double[]) */
    public static  double weightedSum( int[] values, double[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],float[]) */
    public static  double weightedSum( int[] values, float[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],int[]) */
    public static  double weightedSum( int[] values, int[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],long[]) */
    public static  double weightedSum( int[] values, long[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],short[]) */
    public static  double weightedSum( int[] values, short[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( int[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( int[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( int[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( int[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( int[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],double[]) */
    public static  double weightedSum( long[] values, double[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],float[]) */
    public static  double weightedSum( long[] values, float[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],int[]) */
    public static  double weightedSum( long[] values, int[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],long[]) */
    public static  double weightedSum( long[] values, long[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],short[]) */
    public static  double weightedSum( long[] values, short[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( long[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( long[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( long[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( long[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(long[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( long[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],double[]) */
    public static  double weightedSum( short[] values, double[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],float[]) */
    public static  double weightedSum( short[] values, float[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],int[]) */
    public static  double weightedSum( short[] values, int[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],long[]) */
    public static  double weightedSum( short[] values, long[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],short[]) */
    public static  double weightedSum( short[] values, short[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( short[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( short[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( short[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( short[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( short[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, byte[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,double[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, double[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,float[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, float[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,int[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, int[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,long[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, long[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,float[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, float[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,int[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, int[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,long[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, long[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,short[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, short[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,double[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, double[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, float[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,int[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, int[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,long[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, long[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,short[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, short[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,double[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, double[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,float[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, float[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, int[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,long[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, long[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,short[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, short[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,double[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, double[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,float[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, float[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,int[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, int[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, long[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,short[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, short[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,double[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, double[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,float[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, float[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,int[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, int[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,long[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, long[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, short[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#weightedSum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double weightedSum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],byte[]) */
    public static  double wstd( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],double[]) */
    public static  double wstd( byte[] values, double[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],float[]) */
    public static  double wstd( byte[] values, float[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],int[]) */
    public static  double wstd( byte[] values, int[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],long[]) */
    public static  double wstd( byte[] values, long[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wstd( byte[] values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( byte[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( byte[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( byte[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(byte[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( byte[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],double[]) */
    public static  double wstd( double[] values, double[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],float[]) */
    public static  double wstd( double[] values, float[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],int[]) */
    public static  double wstd( double[] values, int[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],long[]) */
    public static  double wstd( double[] values, long[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],short[]) */
    public static  double wstd( double[] values, short[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( double[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( double[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( double[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( double[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(double[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( double[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],double[]) */
    public static  double wstd( float[] values, double[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],float[]) */
    public static  double wstd( float[] values, float[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],int[]) */
    public static  double wstd( float[] values, int[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],long[]) */
    public static  double wstd( float[] values, long[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],short[]) */
    public static  double wstd( float[] values, short[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( float[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( float[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( float[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( float[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(float[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( float[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],double[]) */
    public static  double wstd( int[] values, double[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],float[]) */
    public static  double wstd( int[] values, float[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],int[]) */
    public static  double wstd( int[] values, int[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],long[]) */
    public static  double wstd( int[] values, long[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],short[]) */
    public static  double wstd( int[] values, short[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( int[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( int[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( int[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( int[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(int[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( int[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],double[]) */
    public static  double wstd( long[] values, double[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],float[]) */
    public static  double wstd( long[] values, float[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],int[]) */
    public static  double wstd( long[] values, int[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],long[]) */
    public static  double wstd( long[] values, long[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],short[]) */
    public static  double wstd( long[] values, short[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( long[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( long[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( long[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( long[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(long[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( long[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],double[]) */
    public static  double wstd( short[] values, double[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],float[]) */
    public static  double wstd( short[] values, float[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],int[]) */
    public static  double wstd( short[] values, int[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],long[]) */
    public static  double wstd( short[] values, long[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],short[]) */
    public static  double wstd( short[] values, short[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( short[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( short[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( short[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( short[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( short[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, byte[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,double[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, double[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,float[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, float[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,int[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, int[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,long[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, long[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,float[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, float[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,int[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, int[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,long[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, long[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,short[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, short[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,double[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, double[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, float[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,int[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, int[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,long[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, long[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,short[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, short[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,double[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, double[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,float[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, float[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, int[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,long[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, long[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,short[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, short[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,double[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, double[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,float[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, float[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,int[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, int[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, long[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,short[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, short[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,double[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, double[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,float[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, float[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,int[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, int[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,long[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, long[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, short[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wstd(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wstd( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],byte[]) */
    public static  double wste( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],double[]) */
    public static  double wste( byte[] values, double[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],float[]) */
    public static  double wste( byte[] values, float[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],int[]) */
    public static  double wste( byte[] values, int[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],long[]) */
    public static  double wste( byte[] values, long[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wste( byte[] values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( byte[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( byte[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( byte[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(byte[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( byte[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],double[]) */
    public static  double wste( double[] values, double[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],float[]) */
    public static  double wste( double[] values, float[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],int[]) */
    public static  double wste( double[] values, int[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],long[]) */
    public static  double wste( double[] values, long[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],short[]) */
    public static  double wste( double[] values, short[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( double[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( double[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( double[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( double[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(double[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( double[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],double[]) */
    public static  double wste( float[] values, double[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],float[]) */
    public static  double wste( float[] values, float[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],int[]) */
    public static  double wste( float[] values, int[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],long[]) */
    public static  double wste( float[] values, long[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],short[]) */
    public static  double wste( float[] values, short[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( float[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( float[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( float[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( float[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(float[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( float[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],double[]) */
    public static  double wste( int[] values, double[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],float[]) */
    public static  double wste( int[] values, float[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],int[]) */
    public static  double wste( int[] values, int[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],long[]) */
    public static  double wste( int[] values, long[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],short[]) */
    public static  double wste( int[] values, short[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( int[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( int[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( int[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( int[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(int[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( int[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],double[]) */
    public static  double wste( long[] values, double[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],float[]) */
    public static  double wste( long[] values, float[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],int[]) */
    public static  double wste( long[] values, int[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],long[]) */
    public static  double wste( long[] values, long[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],short[]) */
    public static  double wste( long[] values, short[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( long[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( long[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( long[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( long[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(long[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( long[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],double[]) */
    public static  double wste( short[] values, double[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],float[]) */
    public static  double wste( short[] values, float[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],int[]) */
    public static  double wste( short[] values, int[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],long[]) */
    public static  double wste( short[] values, long[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],short[]) */
    public static  double wste( short[] values, short[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( short[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( short[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( short[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( short[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( short[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, byte[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,double[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, double[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,float[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, float[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,int[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, int[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,long[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, long[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,float[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, float[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,int[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, int[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,long[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, long[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,short[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, short[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,double[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, double[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, float[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,int[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, int[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,long[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, long[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,short[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, short[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,double[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, double[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,float[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, float[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, int[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,long[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, long[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,short[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, short[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,double[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, double[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,float[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, float[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,int[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, int[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, long[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,short[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, short[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,double[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, double[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,float[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, float[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,int[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, int[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,long[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, long[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, short[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wste(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wste( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],byte[]) */
    public static  double wsum( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],double[]) */
    public static  double wsum( byte[] values, double[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],float[]) */
    public static  double wsum( byte[] values, float[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],int[]) */
    public static  double wsum( byte[] values, int[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],long[]) */
    public static  double wsum( byte[] values, long[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wsum( byte[] values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( byte[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( byte[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( byte[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(byte[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( byte[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],double[]) */
    public static  double wsum( double[] values, double[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],float[]) */
    public static  double wsum( double[] values, float[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],int[]) */
    public static  double wsum( double[] values, int[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],long[]) */
    public static  double wsum( double[] values, long[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],short[]) */
    public static  double wsum( double[] values, short[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( double[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( double[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( double[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( double[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(double[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( double[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],double[]) */
    public static  double wsum( float[] values, double[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],float[]) */
    public static  double wsum( float[] values, float[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],int[]) */
    public static  double wsum( float[] values, int[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],long[]) */
    public static  double wsum( float[] values, long[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],short[]) */
    public static  double wsum( float[] values, short[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( float[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( float[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( float[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( float[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(float[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( float[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],double[]) */
    public static  double wsum( int[] values, double[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],float[]) */
    public static  double wsum( int[] values, float[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],int[]) */
    public static  double wsum( int[] values, int[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],long[]) */
    public static  double wsum( int[] values, long[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],short[]) */
    public static  double wsum( int[] values, short[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( int[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( int[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( int[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( int[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(int[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( int[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],double[]) */
    public static  double wsum( long[] values, double[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],float[]) */
    public static  double wsum( long[] values, float[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],int[]) */
    public static  double wsum( long[] values, int[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],long[]) */
    public static  double wsum( long[] values, long[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],short[]) */
    public static  double wsum( long[] values, short[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( long[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( long[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( long[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( long[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(long[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( long[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],double[]) */
    public static  double wsum( short[] values, double[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],float[]) */
    public static  double wsum( short[] values, float[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],int[]) */
    public static  double wsum( short[] values, int[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],long[]) */
    public static  double wsum( short[] values, long[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],short[]) */
    public static  double wsum( short[] values, short[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( short[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( short[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( short[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( short[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( short[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, byte[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,double[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, double[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,float[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, float[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,int[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, int[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,long[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, long[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,float[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, float[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,int[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, int[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,long[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, long[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,short[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, short[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,double[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, double[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, float[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,int[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, int[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,long[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, long[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,short[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, short[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,double[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, double[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,float[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, float[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, int[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,long[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, long[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,short[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, short[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,double[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, double[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,float[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, float[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,int[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, int[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, long[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,short[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, short[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,double[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, double[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,float[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, float[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,int[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, int[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,long[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, long[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, short[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wsum(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wsum( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],byte[]) */
    public static  double wtstat( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],double[]) */
    public static  double wtstat( byte[] values, double[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],float[]) */
    public static  double wtstat( byte[] values, float[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],int[]) */
    public static  double wtstat( byte[] values, int[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],long[]) */
    public static  double wtstat( byte[] values, long[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wtstat( byte[] values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( byte[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( byte[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( byte[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(byte[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( byte[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],double[]) */
    public static  double wtstat( double[] values, double[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],float[]) */
    public static  double wtstat( double[] values, float[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],int[]) */
    public static  double wtstat( double[] values, int[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],long[]) */
    public static  double wtstat( double[] values, long[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],short[]) */
    public static  double wtstat( double[] values, short[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( double[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( double[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( double[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( double[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(double[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( double[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],double[]) */
    public static  double wtstat( float[] values, double[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],float[]) */
    public static  double wtstat( float[] values, float[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],int[]) */
    public static  double wtstat( float[] values, int[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],long[]) */
    public static  double wtstat( float[] values, long[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],short[]) */
    public static  double wtstat( float[] values, short[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( float[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( float[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( float[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( float[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(float[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( float[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],double[]) */
    public static  double wtstat( int[] values, double[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],float[]) */
    public static  double wtstat( int[] values, float[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],int[]) */
    public static  double wtstat( int[] values, int[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],long[]) */
    public static  double wtstat( int[] values, long[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],short[]) */
    public static  double wtstat( int[] values, short[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( int[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( int[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( int[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( int[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(int[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( int[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],double[]) */
    public static  double wtstat( long[] values, double[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],float[]) */
    public static  double wtstat( long[] values, float[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],int[]) */
    public static  double wtstat( long[] values, int[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],long[]) */
    public static  double wtstat( long[] values, long[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],short[]) */
    public static  double wtstat( long[] values, short[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( long[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( long[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( long[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( long[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(long[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( long[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],double[]) */
    public static  double wtstat( short[] values, double[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],float[]) */
    public static  double wtstat( short[] values, float[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],int[]) */
    public static  double wtstat( short[] values, int[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],long[]) */
    public static  double wtstat( short[] values, long[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],short[]) */
    public static  double wtstat( short[] values, short[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( short[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( short[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( short[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( short[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( short[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, byte[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,double[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, double[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,float[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, float[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,int[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, int[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,long[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, long[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,float[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, float[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,int[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, int[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,long[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, long[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,short[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, short[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,double[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, double[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, float[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,int[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, int[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,long[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, long[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,short[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, short[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,double[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, double[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,float[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, float[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, int[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,long[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, long[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,short[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, short[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,double[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, double[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,float[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, float[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,int[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, int[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, long[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,short[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, short[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,double[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, double[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,float[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, float[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,int[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, int[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,long[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, long[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, short[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wtstat(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wtstat( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],byte[]) */
    public static  double wvar( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],double[]) */
    public static  double wvar( byte[] values, double[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],float[]) */
    public static  double wvar( byte[] values, float[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],int[]) */
    public static  double wvar( byte[] values, int[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],long[]) */
    public static  double wvar( byte[] values, long[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wvar( byte[] values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( byte[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( byte[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( byte[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(byte[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( byte[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],double[]) */
    public static  double wvar( double[] values, double[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],float[]) */
    public static  double wvar( double[] values, float[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],int[]) */
    public static  double wvar( double[] values, int[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],long[]) */
    public static  double wvar( double[] values, long[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],short[]) */
    public static  double wvar( double[] values, short[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( double[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( double[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( double[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( double[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(double[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( double[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],double[]) */
    public static  double wvar( float[] values, double[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],float[]) */
    public static  double wvar( float[] values, float[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],int[]) */
    public static  double wvar( float[] values, int[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],long[]) */
    public static  double wvar( float[] values, long[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],short[]) */
    public static  double wvar( float[] values, short[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( float[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( float[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( float[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( float[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(float[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( float[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],double[]) */
    public static  double wvar( int[] values, double[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],float[]) */
    public static  double wvar( int[] values, float[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],int[]) */
    public static  double wvar( int[] values, int[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],long[]) */
    public static  double wvar( int[] values, long[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],short[]) */
    public static  double wvar( int[] values, short[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( int[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( int[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( int[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( int[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(int[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( int[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],double[]) */
    public static  double wvar( long[] values, double[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],float[]) */
    public static  double wvar( long[] values, float[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],int[]) */
    public static  double wvar( long[] values, int[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],long[]) */
    public static  double wvar( long[] values, long[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],short[]) */
    public static  double wvar( long[] values, short[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( long[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( long[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( long[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( long[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(long[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( long[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],double[]) */
    public static  double wvar( short[] values, double[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],float[]) */
    public static  double wvar( short[] values, float[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],int[]) */
    public static  double wvar( short[] values, int[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],long[]) */
    public static  double wvar( short[] values, long[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],short[]) */
    public static  double wvar( short[] values, short[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( short[] values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( short[] values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( short[] values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( short[] values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(short[],io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( short[] values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,byte[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, byte[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,double[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, double[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,float[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, float[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,int[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, int[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,long[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, long[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbByteArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbByteArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ByteNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbByteArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbByteArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,double[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, double[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,float[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, float[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,int[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, int[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,long[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, long[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,short[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, short[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.DoubleNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbDoubleArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbDoubleArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,double[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, double[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,float[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, float[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,int[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, int[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,long[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, long[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,short[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, short[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.FloatNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbFloatArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbFloatArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,double[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, double[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,float[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, float[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,int[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, int[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,long[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, long[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,short[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, short[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.IntegerNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbIntArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbIntArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,double[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, double[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,float[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, float[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,int[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, int[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,long[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, long[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,short[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, short[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.LongNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbLongArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbLongArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,double[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, double[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,float[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, float[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,int[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, int[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,long[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, long[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,short[]) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, short[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbDoubleArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbDoubleArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbFloatArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbFloatArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbIntArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbIntArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbLongArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbLongArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.libs.primitives.ShortNumericPrimitives#wvar(io.deephaven.db.tables.dbarrays.DbShortArray,io.deephaven.db.tables.dbarrays.DbShortArray) */
    public static  double wvar( io.deephaven.db.tables.dbarrays.DbShortArray values, io.deephaven.db.tables.dbarrays.DbShortArray weights ) {return ShortNumericPrimitives.wvar( values, weights );}
}

