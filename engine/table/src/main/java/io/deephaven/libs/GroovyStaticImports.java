/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/****************************************************************************************************************************
 ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - Run GroovyStaticImportGenerator or "./gradlew :Generators:groovyStaticImportGenerator" to regenerate
 ****************************************************************************************************************************/

package io.deephaven.libs;

import io.deephaven.function.BinSearch;
import io.deephaven.function.BooleanPrimitives;
import io.deephaven.function.ByteNumericPrimitives;
import io.deephaven.function.BytePrimitives;
import io.deephaven.function.Casting;
import io.deephaven.function.CharacterPrimitives;
import io.deephaven.function.ComparePrimitives;
import io.deephaven.function.DoubleFpPrimitives;
import io.deephaven.function.DoubleNumericPrimitives;
import io.deephaven.function.DoublePrimitives;
import io.deephaven.function.FloatFpPrimitives;
import io.deephaven.function.FloatNumericPrimitives;
import io.deephaven.function.FloatPrimitives;
import io.deephaven.function.IntegerNumericPrimitives;
import io.deephaven.function.IntegerPrimitives;
import io.deephaven.function.LongNumericPrimitives;
import io.deephaven.function.LongPrimitives;
import io.deephaven.function.ObjectPrimitives;
import io.deephaven.function.ShortNumericPrimitives;
import io.deephaven.function.ShortPrimitives;
import io.deephaven.function.SpecialPrimitives;
import io.deephaven.vector.BooleanVector;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
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
 * @see io.deephaven.function
 */
public class GroovyStaticImports {
    /** @see io.deephaven.function.ByteNumericPrimitives#abs(byte) */
    public static  byte abs( byte value ) {return ByteNumericPrimitives.abs( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#abs(double) */
    public static  double abs( double value ) {return DoubleNumericPrimitives.abs( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#abs(float) */
    public static  float abs( float value ) {return FloatNumericPrimitives.abs( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#abs(int) */
    public static  int abs( int value ) {return IntegerNumericPrimitives.abs( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#abs(long) */
    public static  long abs( long value ) {return LongNumericPrimitives.abs( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#abs(short) */
    public static  short abs( short value ) {return ShortNumericPrimitives.abs( value );}
    /** @see io.deephaven.function.ByteNumericPrimitives#absAvg(byte[]) */
    public static  double absAvg( byte[] values ) {return ByteNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#absAvg(double[]) */
    public static  double absAvg( double[] values ) {return DoubleNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#absAvg(float[]) */
    public static  double absAvg( float[] values ) {return FloatNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#absAvg(int[]) */
    public static  double absAvg( int[] values ) {return IntegerNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#absAvg(long[]) */
    public static  double absAvg( long[] values ) {return LongNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#absAvg(java.lang.Byte[]) */
    public static  double absAvg( java.lang.Byte[] values ) {return ByteNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#absAvg(java.lang.Double[]) */
    public static  double absAvg( java.lang.Double[] values ) {return DoubleNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#absAvg(java.lang.Float[]) */
    public static  double absAvg( java.lang.Float[] values ) {return FloatNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#absAvg(java.lang.Integer[]) */
    public static  double absAvg( java.lang.Integer[] values ) {return IntegerNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#absAvg(java.lang.Long[]) */
    public static  double absAvg( java.lang.Long[] values ) {return LongNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#absAvg(java.lang.Short[]) */
    public static  double absAvg( java.lang.Short[] values ) {return ShortNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#absAvg(short[]) */
    public static  double absAvg( short[] values ) {return ShortNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#absAvg(io.deephaven.vector.ByteVector) */
    public static  double absAvg( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#absAvg(io.deephaven.vector.DoubleVector) */
    public static  double absAvg( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#absAvg(io.deephaven.vector.FloatVector) */
    public static  double absAvg( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#absAvg(io.deephaven.vector.IntVector) */
    public static  double absAvg( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#absAvg(io.deephaven.vector.LongVector) */
    public static  double absAvg( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#absAvg(io.deephaven.vector.ShortVector) */
    public static  double absAvg( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.absAvg( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#acos(byte) */
    public static  double acos( byte value ) {return ByteNumericPrimitives.acos( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#acos(double) */
    public static  double acos( double value ) {return DoubleNumericPrimitives.acos( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#acos(float) */
    public static  double acos( float value ) {return FloatNumericPrimitives.acos( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#acos(int) */
    public static  double acos( int value ) {return IntegerNumericPrimitives.acos( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#acos(long) */
    public static  double acos( long value ) {return LongNumericPrimitives.acos( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#acos(short) */
    public static  double acos( short value ) {return ShortNumericPrimitives.acos( value );}
    /** @see io.deephaven.function.BooleanPrimitives#and(java.lang.Boolean[]) */
    public static  java.lang.Boolean and( java.lang.Boolean[] values ) {return BooleanPrimitives.and( values );}
    /** @see io.deephaven.function.BooleanPrimitives#and(boolean[]) */
    public static  java.lang.Boolean and( boolean[] values ) {return BooleanPrimitives.and( values );}
    /** @see io.deephaven.function.BooleanPrimitives#and(io.deephaven.vector.ObjectVector<java.lang.Boolean>) */
    public static  java.lang.Boolean and( io.deephaven.vector.ObjectVector<java.lang.Boolean> values ) {return BooleanPrimitives.and( values );}
    /** @see io.deephaven.function.BooleanPrimitives#and(java.lang.Boolean[],java.lang.Boolean) */
    public static  java.lang.Boolean and( java.lang.Boolean[] values, java.lang.Boolean nullValue ) {return BooleanPrimitives.and( values, nullValue );}
    /** @see io.deephaven.function.BooleanPrimitives#and(io.deephaven.vector.ObjectVector<java.lang.Boolean>,java.lang.Boolean) */
    public static  java.lang.Boolean and( io.deephaven.vector.ObjectVector<java.lang.Boolean> values, java.lang.Boolean nullValue ) {return BooleanPrimitives.and( values, nullValue );}
    /** @see io.deephaven.function.BytePrimitives#array(byte[]) */
    public static  io.deephaven.vector.ByteVector array( byte[] values ) {return BytePrimitives.array( values );}
    /** @see io.deephaven.function.CharacterPrimitives#array(char[]) */
    public static  io.deephaven.vector.CharVector array( char[] values ) {return CharacterPrimitives.array( values );}
    /** @see io.deephaven.function.DoublePrimitives#array(double[]) */
    public static  io.deephaven.vector.DoubleVector array( double[] values ) {return DoublePrimitives.array( values );}
    /** @see io.deephaven.function.FloatPrimitives#array(float[]) */
    public static  io.deephaven.vector.FloatVector array( float[] values ) {return FloatPrimitives.array( values );}
    /** @see io.deephaven.function.IntegerPrimitives#array(int[]) */
    public static  io.deephaven.vector.IntVector array( int[] values ) {return IntegerPrimitives.array( values );}
    /** @see io.deephaven.function.LongPrimitives#array(long[]) */
    public static  io.deephaven.vector.LongVector array( long[] values ) {return LongPrimitives.array( values );}
    /** @see io.deephaven.function.BooleanPrimitives#array(java.lang.Boolean[]) */
    public static  io.deephaven.vector.BooleanVector array( java.lang.Boolean[] values ) {return BooleanPrimitives.array( values );}
    /** @see io.deephaven.function.ShortPrimitives#array(short[]) */
    public static  io.deephaven.vector.ShortVector array( short[] values ) {return ShortPrimitives.array( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#asin(byte) */
    public static  double asin( byte value ) {return ByteNumericPrimitives.asin( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#asin(double) */
    public static  double asin( double value ) {return DoubleNumericPrimitives.asin( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#asin(float) */
    public static  double asin( float value ) {return FloatNumericPrimitives.asin( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#asin(int) */
    public static  double asin( int value ) {return IntegerNumericPrimitives.asin( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#asin(long) */
    public static  double asin( long value ) {return LongNumericPrimitives.asin( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#asin(short) */
    public static  double asin( short value ) {return ShortNumericPrimitives.asin( value );}
    /** @see io.deephaven.function.ByteNumericPrimitives#atan(byte) */
    public static  double atan( byte value ) {return ByteNumericPrimitives.atan( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#atan(double) */
    public static  double atan( double value ) {return DoubleNumericPrimitives.atan( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#atan(float) */
    public static  double atan( float value ) {return FloatNumericPrimitives.atan( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#atan(int) */
    public static  double atan( int value ) {return IntegerNumericPrimitives.atan( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#atan(long) */
    public static  double atan( long value ) {return LongNumericPrimitives.atan( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#atan(short) */
    public static  double atan( short value ) {return ShortNumericPrimitives.atan( value );}
    /** @see io.deephaven.function.ByteNumericPrimitives#avg(byte[]) */
    public static  double avg( byte[] values ) {return ByteNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#avg(double[]) */
    public static  double avg( double[] values ) {return DoubleNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#avg(float[]) */
    public static  double avg( float[] values ) {return FloatNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#avg(int[]) */
    public static  double avg( int[] values ) {return IntegerNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#avg(long[]) */
    public static  double avg( long[] values ) {return LongNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#avg(java.lang.Byte[]) */
    public static  double avg( java.lang.Byte[] values ) {return ByteNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#avg(java.lang.Double[]) */
    public static  double avg( java.lang.Double[] values ) {return DoubleNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#avg(java.lang.Float[]) */
    public static  double avg( java.lang.Float[] values ) {return FloatNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#avg(java.lang.Integer[]) */
    public static  double avg( java.lang.Integer[] values ) {return IntegerNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#avg(java.lang.Long[]) */
    public static  double avg( java.lang.Long[] values ) {return LongNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#avg(java.lang.Short[]) */
    public static  double avg( java.lang.Short[] values ) {return ShortNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#avg(short[]) */
    public static  double avg( short[] values ) {return ShortNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#avg(io.deephaven.vector.ByteVector) */
    public static  double avg( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#avg(io.deephaven.vector.DoubleVector) */
    public static  double avg( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#avg(io.deephaven.vector.FloatVector) */
    public static  double avg( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#avg(io.deephaven.vector.IntVector) */
    public static  double avg( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#avg(io.deephaven.vector.LongVector) */
    public static  double avg( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#avg(io.deephaven.vector.ShortVector) */
    public static  double avg( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.avg( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#binSearchIndex(byte[],byte,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( byte[] values, byte key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ByteNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#binSearchIndex(double[],double,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( double[] values, double key, io.deephaven.function.BinSearch choiceWhenEquals ) {return DoubleNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.FloatNumericPrimitives#binSearchIndex(float[],float,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( float[] values, float key, io.deephaven.function.BinSearch choiceWhenEquals ) {return FloatNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#binSearchIndex(int[],int,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( int[] values, int key, io.deephaven.function.BinSearch choiceWhenEquals ) {return IntegerNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.LongNumericPrimitives#binSearchIndex(long[],long,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( long[] values, long key, io.deephaven.function.BinSearch choiceWhenEquals ) {return LongNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.ShortNumericPrimitives#binSearchIndex(short[],short,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( short[] values, short key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ShortNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.ByteNumericPrimitives#binSearchIndex(io.deephaven.vector.ByteVector,byte,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( io.deephaven.vector.ByteVector values, byte key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ByteNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#binSearchIndex(io.deephaven.vector.DoubleVector,double,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( io.deephaven.vector.DoubleVector values, double key, io.deephaven.function.BinSearch choiceWhenEquals ) {return DoubleNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.FloatNumericPrimitives#binSearchIndex(io.deephaven.vector.FloatVector,float,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( io.deephaven.vector.FloatVector values, float key, io.deephaven.function.BinSearch choiceWhenEquals ) {return FloatNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#binSearchIndex(io.deephaven.vector.IntVector,int,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( io.deephaven.vector.IntVector values, int key, io.deephaven.function.BinSearch choiceWhenEquals ) {return IntegerNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.LongNumericPrimitives#binSearchIndex(io.deephaven.vector.LongVector,long,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( io.deephaven.vector.LongVector values, long key, io.deephaven.function.BinSearch choiceWhenEquals ) {return LongNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.ShortNumericPrimitives#binSearchIndex(io.deephaven.vector.ShortVector,short,io.deephaven.function.BinSearch) */
    public static  int binSearchIndex( io.deephaven.vector.ShortVector values, short key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ShortNumericPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.ObjectPrimitives#binSearchIndex(io.deephaven.vector.ObjectVector,T,io.deephaven.function.BinSearch) */
    public static <T extends java.lang.Comparable<? super T>> int binSearchIndex( io.deephaven.vector.ObjectVector<T> values, T key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ObjectPrimitives.binSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.Casting#castDouble(byte[]) */
    public static  double[] castDouble( byte[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(double[]) */
    public static  double[] castDouble( double[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(float[]) */
    public static  double[] castDouble( float[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(int[]) */
    public static  double[] castDouble( int[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(long[]) */
    public static  double[] castDouble( long[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(short[]) */
    public static  double[] castDouble( short[] values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(io.deephaven.vector.ByteVector) */
    public static  double[] castDouble( io.deephaven.vector.ByteVector values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(io.deephaven.vector.DoubleVector) */
    public static  double[] castDouble( io.deephaven.vector.DoubleVector values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(io.deephaven.vector.FloatVector) */
    public static  double[] castDouble( io.deephaven.vector.FloatVector values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(io.deephaven.vector.IntVector) */
    public static  double[] castDouble( io.deephaven.vector.IntVector values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(io.deephaven.vector.LongVector) */
    public static  double[] castDouble( io.deephaven.vector.LongVector values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castDouble(io.deephaven.vector.ShortVector) */
    public static  double[] castDouble( io.deephaven.vector.ShortVector values ) {return Casting.castDouble( values );}
    /** @see io.deephaven.function.Casting#castLong(byte[]) */
    public static  long[] castLong( byte[] values ) {return Casting.castLong( values );}
    /** @see io.deephaven.function.Casting#castLong(int[]) */
    public static  long[] castLong( int[] values ) {return Casting.castLong( values );}
    /** @see io.deephaven.function.Casting#castLong(long[]) */
    public static  long[] castLong( long[] values ) {return Casting.castLong( values );}
    /** @see io.deephaven.function.Casting#castLong(short[]) */
    public static  long[] castLong( short[] values ) {return Casting.castLong( values );}
    /** @see io.deephaven.function.Casting#castLong(io.deephaven.vector.ByteVector) */
    public static  long[] castLong( io.deephaven.vector.ByteVector values ) {return Casting.castLong( values );}
    /** @see io.deephaven.function.Casting#castLong(io.deephaven.vector.IntVector) */
    public static  long[] castLong( io.deephaven.vector.IntVector values ) {return Casting.castLong( values );}
    /** @see io.deephaven.function.Casting#castLong(io.deephaven.vector.LongVector) */
    public static  long[] castLong( io.deephaven.vector.LongVector values ) {return Casting.castLong( values );}
    /** @see io.deephaven.function.Casting#castLong(io.deephaven.vector.ShortVector) */
    public static  long[] castLong( io.deephaven.vector.ShortVector values ) {return Casting.castLong( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#ceil(byte) */
    public static  double ceil( byte value ) {return ByteNumericPrimitives.ceil( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#ceil(double) */
    public static  double ceil( double value ) {return DoubleNumericPrimitives.ceil( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#ceil(float) */
    public static  double ceil( float value ) {return FloatNumericPrimitives.ceil( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#ceil(int) */
    public static  double ceil( int value ) {return IntegerNumericPrimitives.ceil( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#ceil(long) */
    public static  double ceil( long value ) {return LongNumericPrimitives.ceil( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#ceil(short) */
    public static  double ceil( short value ) {return ShortNumericPrimitives.ceil( value );}
    /** @see io.deephaven.function.ByteNumericPrimitives#clamp(byte,byte,byte) */
    public static  byte clamp( byte value, byte min, byte max ) {return ByteNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#clamp(double,double,double) */
    public static  double clamp( double value, double min, double max ) {return DoubleNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.function.FloatNumericPrimitives#clamp(float,float,float) */
    public static  float clamp( float value, float min, float max ) {return FloatNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#clamp(int,int,int) */
    public static  int clamp( int value, int min, int max ) {return IntegerNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.function.LongNumericPrimitives#clamp(long,long,long) */
    public static  long clamp( long value, long min, long max ) {return LongNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.function.ShortNumericPrimitives#clamp(short,short,short) */
    public static  short clamp( short value, short min, short max ) {return ShortNumericPrimitives.clamp( value, min, max );}
    /** @see io.deephaven.function.BytePrimitives#concat(io.deephaven.vector.ByteVector[]) */
    public static  byte[] concat( io.deephaven.vector.ByteVector[] values ) {return BytePrimitives.concat( values );}
    /** @see io.deephaven.function.CharacterPrimitives#concat(io.deephaven.vector.CharVector[]) */
    public static  char[] concat( io.deephaven.vector.CharVector[] values ) {return CharacterPrimitives.concat( values );}
    /** @see io.deephaven.function.DoublePrimitives#concat(io.deephaven.vector.DoubleVector[]) */
    public static  double[] concat( io.deephaven.vector.DoubleVector[] values ) {return DoublePrimitives.concat( values );}
    /** @see io.deephaven.function.FloatPrimitives#concat(io.deephaven.vector.FloatVector[]) */
    public static  float[] concat( io.deephaven.vector.FloatVector[] values ) {return FloatPrimitives.concat( values );}
    /** @see io.deephaven.function.IntegerPrimitives#concat(io.deephaven.vector.IntVector[]) */
    public static  int[] concat( io.deephaven.vector.IntVector[] values ) {return IntegerPrimitives.concat( values );}
    /** @see io.deephaven.function.LongPrimitives#concat(io.deephaven.vector.LongVector[]) */
    public static  long[] concat( io.deephaven.vector.LongVector[] values ) {return LongPrimitives.concat( values );}
    /** @see io.deephaven.function.ShortPrimitives#concat(io.deephaven.vector.ShortVector[]) */
    public static  short[] concat( io.deephaven.vector.ShortVector[] values ) {return ShortPrimitives.concat( values );}
    /** @see io.deephaven.function.BytePrimitives#concat(byte[][]) */
    public static  byte[] concat( byte[][] values ) {return BytePrimitives.concat( values );}
    /** @see io.deephaven.function.CharacterPrimitives#concat(char[][]) */
    public static  char[] concat( char[][] values ) {return CharacterPrimitives.concat( values );}
    /** @see io.deephaven.function.DoublePrimitives#concat(double[][]) */
    public static  double[] concat( double[][] values ) {return DoublePrimitives.concat( values );}
    /** @see io.deephaven.function.FloatPrimitives#concat(float[][]) */
    public static  float[] concat( float[][] values ) {return FloatPrimitives.concat( values );}
    /** @see io.deephaven.function.IntegerPrimitives#concat(int[][]) */
    public static  int[] concat( int[][] values ) {return IntegerPrimitives.concat( values );}
    /** @see io.deephaven.function.LongPrimitives#concat(long[][]) */
    public static  long[] concat( long[][] values ) {return LongPrimitives.concat( values );}
    /** @see io.deephaven.function.ShortPrimitives#concat(short[][]) */
    public static  short[] concat( short[][] values ) {return ShortPrimitives.concat( values );}
    /** @see io.deephaven.function.ObjectPrimitives#contains(java.lang.String,java.lang.CharSequence) */
    public static  boolean contains( java.lang.String target, java.lang.CharSequence sequence ) {return ObjectPrimitives.contains( target, sequence );}
    /** @see io.deephaven.function.DoubleFpPrimitives#containsNonNormal(double[]) */
    public static  boolean containsNonNormal( double[] values ) {return DoubleFpPrimitives.containsNonNormal( values );}
    /** @see io.deephaven.function.FloatFpPrimitives#containsNonNormal(float[]) */
    public static  boolean containsNonNormal( float[] values ) {return FloatFpPrimitives.containsNonNormal( values );}
    /** @see io.deephaven.function.DoubleFpPrimitives#containsNonNormal(java.lang.Double[]) */
    public static  boolean containsNonNormal( java.lang.Double[] values ) {return DoubleFpPrimitives.containsNonNormal( values );}
    /** @see io.deephaven.function.FloatFpPrimitives#containsNonNormal(java.lang.Float[]) */
    public static  boolean containsNonNormal( java.lang.Float[] values ) {return FloatFpPrimitives.containsNonNormal( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cor(byte[],byte[]) */
    public static  double cor( byte[] values0, byte[] values1 ) {return ByteNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cor(byte[],io.deephaven.vector.ByteVector) */
    public static  double cor( byte[] values0, io.deephaven.vector.ByteVector values1 ) {return ByteNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cor(double[],double[]) */
    public static  double cor( double[] values0, double[] values1 ) {return DoubleNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cor(double[],io.deephaven.vector.DoubleVector) */
    public static  double cor( double[] values0, io.deephaven.vector.DoubleVector values1 ) {return DoubleNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cor(float[],float[]) */
    public static  double cor( float[] values0, float[] values1 ) {return FloatNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cor(float[],io.deephaven.vector.FloatVector) */
    public static  double cor( float[] values0, io.deephaven.vector.FloatVector values1 ) {return FloatNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cor(int[],int[]) */
    public static  double cor( int[] values0, int[] values1 ) {return IntegerNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cor(int[],io.deephaven.vector.IntVector) */
    public static  double cor( int[] values0, io.deephaven.vector.IntVector values1 ) {return IntegerNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.LongNumericPrimitives#cor(long[],long[]) */
    public static  double cor( long[] values0, long[] values1 ) {return LongNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.LongNumericPrimitives#cor(long[],io.deephaven.vector.LongVector) */
    public static  double cor( long[] values0, io.deephaven.vector.LongVector values1 ) {return LongNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cor(short[],short[]) */
    public static  double cor( short[] values0, short[] values1 ) {return ShortNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cor(short[],io.deephaven.vector.ShortVector) */
    public static  double cor( short[] values0, io.deephaven.vector.ShortVector values1 ) {return ShortNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cor(io.deephaven.vector.ByteVector,byte[]) */
    public static  double cor( io.deephaven.vector.ByteVector values0, byte[] values1 ) {return ByteNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cor(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double cor( io.deephaven.vector.ByteVector values0, io.deephaven.vector.ByteVector values1 ) {return ByteNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cor(io.deephaven.vector.DoubleVector,double[]) */
    public static  double cor( io.deephaven.vector.DoubleVector values0, double[] values1 ) {return DoubleNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cor(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double cor( io.deephaven.vector.DoubleVector values0, io.deephaven.vector.DoubleVector values1 ) {return DoubleNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cor(io.deephaven.vector.FloatVector,float[]) */
    public static  double cor( io.deephaven.vector.FloatVector values0, float[] values1 ) {return FloatNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cor(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double cor( io.deephaven.vector.FloatVector values0, io.deephaven.vector.FloatVector values1 ) {return FloatNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cor(io.deephaven.vector.IntVector,int[]) */
    public static  double cor( io.deephaven.vector.IntVector values0, int[] values1 ) {return IntegerNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cor(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double cor( io.deephaven.vector.IntVector values0, io.deephaven.vector.IntVector values1 ) {return IntegerNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.LongNumericPrimitives#cor(io.deephaven.vector.LongVector,long[]) */
    public static  double cor( io.deephaven.vector.LongVector values0, long[] values1 ) {return LongNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.LongNumericPrimitives#cor(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double cor( io.deephaven.vector.LongVector values0, io.deephaven.vector.LongVector values1 ) {return LongNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cor(io.deephaven.vector.ShortVector,short[]) */
    public static  double cor( io.deephaven.vector.ShortVector values0, short[] values1 ) {return ShortNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cor(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double cor( io.deephaven.vector.ShortVector values0, io.deephaven.vector.ShortVector values1 ) {return ShortNumericPrimitives.cor( values0, values1 );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cos(byte) */
    public static  double cos( byte value ) {return ByteNumericPrimitives.cos( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cos(double) */
    public static  double cos( double value ) {return DoubleNumericPrimitives.cos( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cos(float) */
    public static  double cos( float value ) {return FloatNumericPrimitives.cos( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cos(int) */
    public static  double cos( int value ) {return IntegerNumericPrimitives.cos( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#cos(long) */
    public static  double cos( long value ) {return LongNumericPrimitives.cos( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cos(short) */
    public static  double cos( short value ) {return ShortNumericPrimitives.cos( value );}
    /** @see io.deephaven.function.BytePrimitives#count(byte[]) */
    public static  int count( byte[] values ) {return BytePrimitives.count( values );}
    /** @see io.deephaven.function.CharacterPrimitives#count(char[]) */
    public static  int count( char[] values ) {return CharacterPrimitives.count( values );}
    /** @see io.deephaven.function.DoublePrimitives#count(double[]) */
    public static  int count( double[] values ) {return DoublePrimitives.count( values );}
    /** @see io.deephaven.function.FloatPrimitives#count(float[]) */
    public static  int count( float[] values ) {return FloatPrimitives.count( values );}
    /** @see io.deephaven.function.IntegerPrimitives#count(int[]) */
    public static  int count( int[] values ) {return IntegerPrimitives.count( values );}
    /** @see io.deephaven.function.LongPrimitives#count(long[]) */
    public static  int count( long[] values ) {return LongPrimitives.count( values );}
    /** @see io.deephaven.function.BytePrimitives#count(java.lang.Byte[]) */
    public static  int count( java.lang.Byte[] values ) {return BytePrimitives.count( values );}
    /** @see io.deephaven.function.CharacterPrimitives#count(java.lang.Character[]) */
    public static  int count( java.lang.Character[] values ) {return CharacterPrimitives.count( values );}
    /** @see io.deephaven.function.DoublePrimitives#count(java.lang.Double[]) */
    public static  int count( java.lang.Double[] values ) {return DoublePrimitives.count( values );}
    /** @see io.deephaven.function.FloatPrimitives#count(java.lang.Float[]) */
    public static  int count( java.lang.Float[] values ) {return FloatPrimitives.count( values );}
    /** @see io.deephaven.function.IntegerPrimitives#count(java.lang.Integer[]) */
    public static  int count( java.lang.Integer[] values ) {return IntegerPrimitives.count( values );}
    /** @see io.deephaven.function.LongPrimitives#count(java.lang.Long[]) */
    public static  int count( java.lang.Long[] values ) {return LongPrimitives.count( values );}
    /** @see io.deephaven.function.ShortPrimitives#count(java.lang.Short[]) */
    public static  int count( java.lang.Short[] values ) {return ShortPrimitives.count( values );}
    /** @see io.deephaven.function.ShortPrimitives#count(short[]) */
    public static  int count( short[] values ) {return ShortPrimitives.count( values );}
    /** @see io.deephaven.function.BooleanPrimitives#count(io.deephaven.vector.BooleanVector) */
    public static  int count( io.deephaven.vector.BooleanVector values ) {return BooleanPrimitives.count( values );}
    /** @see io.deephaven.function.BytePrimitives#count(io.deephaven.vector.ByteVector) */
    public static  int count( io.deephaven.vector.ByteVector values ) {return BytePrimitives.count( values );}
    /** @see io.deephaven.function.CharacterPrimitives#count(io.deephaven.vector.CharVector) */
    public static  int count( io.deephaven.vector.CharVector values ) {return CharacterPrimitives.count( values );}
    /** @see io.deephaven.function.DoublePrimitives#count(io.deephaven.vector.DoubleVector) */
    public static  int count( io.deephaven.vector.DoubleVector values ) {return DoublePrimitives.count( values );}
    /** @see io.deephaven.function.FloatPrimitives#count(io.deephaven.vector.FloatVector) */
    public static  int count( io.deephaven.vector.FloatVector values ) {return FloatPrimitives.count( values );}
    /** @see io.deephaven.function.IntegerPrimitives#count(io.deephaven.vector.IntVector) */
    public static  int count( io.deephaven.vector.IntVector values ) {return IntegerPrimitives.count( values );}
    /** @see io.deephaven.function.LongPrimitives#count(io.deephaven.vector.LongVector) */
    public static  int count( io.deephaven.vector.LongVector values ) {return LongPrimitives.count( values );}
    /** @see io.deephaven.function.ShortPrimitives#count(io.deephaven.vector.ShortVector) */
    public static  int count( io.deephaven.vector.ShortVector values ) {return ShortPrimitives.count( values );}
    /** @see io.deephaven.function.ObjectPrimitives#count(io.deephaven.vector.ObjectVector) */
    public static <T> int count( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.count( values );}
    /** @see io.deephaven.function.BytePrimitives#countDistinct(byte[]) */
    public static  long countDistinct( byte[] values ) {return BytePrimitives.countDistinct( values );}
    /** @see io.deephaven.function.CharacterPrimitives#countDistinct(char[]) */
    public static  long countDistinct( char[] values ) {return CharacterPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.DoublePrimitives#countDistinct(double[]) */
    public static  long countDistinct( double[] values ) {return DoublePrimitives.countDistinct( values );}
    /** @see io.deephaven.function.FloatPrimitives#countDistinct(float[]) */
    public static  long countDistinct( float[] values ) {return FloatPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.IntegerPrimitives#countDistinct(int[]) */
    public static  long countDistinct( int[] values ) {return IntegerPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.LongPrimitives#countDistinct(long[]) */
    public static  long countDistinct( long[] values ) {return LongPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.ShortPrimitives#countDistinct(short[]) */
    public static  long countDistinct( short[] values ) {return ShortPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.BytePrimitives#countDistinct(io.deephaven.vector.ByteVector) */
    public static  long countDistinct( io.deephaven.vector.ByteVector values ) {return BytePrimitives.countDistinct( values );}
    /** @see io.deephaven.function.CharacterPrimitives#countDistinct(io.deephaven.vector.CharVector) */
    public static  long countDistinct( io.deephaven.vector.CharVector values ) {return CharacterPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.DoublePrimitives#countDistinct(io.deephaven.vector.DoubleVector) */
    public static  long countDistinct( io.deephaven.vector.DoubleVector values ) {return DoublePrimitives.countDistinct( values );}
    /** @see io.deephaven.function.FloatPrimitives#countDistinct(io.deephaven.vector.FloatVector) */
    public static  long countDistinct( io.deephaven.vector.FloatVector values ) {return FloatPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.IntegerPrimitives#countDistinct(io.deephaven.vector.IntVector) */
    public static  long countDistinct( io.deephaven.vector.IntVector values ) {return IntegerPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.LongPrimitives#countDistinct(io.deephaven.vector.LongVector) */
    public static  long countDistinct( io.deephaven.vector.LongVector values ) {return LongPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.ShortPrimitives#countDistinct(io.deephaven.vector.ShortVector) */
    public static  long countDistinct( io.deephaven.vector.ShortVector values ) {return ShortPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.ObjectPrimitives#countDistinct(io.deephaven.vector.ObjectVector) */
    public static <T extends java.lang.Comparable<? super T>> long countDistinct( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.countDistinct( values );}
    /** @see io.deephaven.function.BytePrimitives#countDistinct(byte[],boolean) */
    public static  long countDistinct( byte[] values, boolean countNull ) {return BytePrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.CharacterPrimitives#countDistinct(char[],boolean) */
    public static  long countDistinct( char[] values, boolean countNull ) {return CharacterPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.DoublePrimitives#countDistinct(double[],boolean) */
    public static  long countDistinct( double[] values, boolean countNull ) {return DoublePrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.FloatPrimitives#countDistinct(float[],boolean) */
    public static  long countDistinct( float[] values, boolean countNull ) {return FloatPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.IntegerPrimitives#countDistinct(int[],boolean) */
    public static  long countDistinct( int[] values, boolean countNull ) {return IntegerPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.LongPrimitives#countDistinct(long[],boolean) */
    public static  long countDistinct( long[] values, boolean countNull ) {return LongPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.ShortPrimitives#countDistinct(short[],boolean) */
    public static  long countDistinct( short[] values, boolean countNull ) {return ShortPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.BytePrimitives#countDistinct(io.deephaven.vector.ByteVector,boolean) */
    public static  long countDistinct( io.deephaven.vector.ByteVector values, boolean countNull ) {return BytePrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.CharacterPrimitives#countDistinct(io.deephaven.vector.CharVector,boolean) */
    public static  long countDistinct( io.deephaven.vector.CharVector values, boolean countNull ) {return CharacterPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.DoublePrimitives#countDistinct(io.deephaven.vector.DoubleVector,boolean) */
    public static  long countDistinct( io.deephaven.vector.DoubleVector values, boolean countNull ) {return DoublePrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.FloatPrimitives#countDistinct(io.deephaven.vector.FloatVector,boolean) */
    public static  long countDistinct( io.deephaven.vector.FloatVector values, boolean countNull ) {return FloatPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.IntegerPrimitives#countDistinct(io.deephaven.vector.IntVector,boolean) */
    public static  long countDistinct( io.deephaven.vector.IntVector values, boolean countNull ) {return IntegerPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.LongPrimitives#countDistinct(io.deephaven.vector.LongVector,boolean) */
    public static  long countDistinct( io.deephaven.vector.LongVector values, boolean countNull ) {return LongPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.ShortPrimitives#countDistinct(io.deephaven.vector.ShortVector,boolean) */
    public static  long countDistinct( io.deephaven.vector.ShortVector values, boolean countNull ) {return ShortPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.ObjectPrimitives#countDistinct(io.deephaven.vector.ObjectVector,boolean) */
    public static <T extends java.lang.Comparable<? super T>> long countDistinct( io.deephaven.vector.ObjectVector<T> values, boolean countNull ) {return ObjectPrimitives.countDistinct( values, countNull );}
    /** @see io.deephaven.function.ByteNumericPrimitives#countNeg(byte[]) */
    public static  int countNeg( byte[] values ) {return ByteNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#countNeg(double[]) */
    public static  int countNeg( double[] values ) {return DoubleNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#countNeg(float[]) */
    public static  int countNeg( float[] values ) {return FloatNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#countNeg(int[]) */
    public static  int countNeg( int[] values ) {return IntegerNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#countNeg(long[]) */
    public static  int countNeg( long[] values ) {return LongNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#countNeg(java.lang.Byte[]) */
    public static  int countNeg( java.lang.Byte[] values ) {return ByteNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#countNeg(java.lang.Double[]) */
    public static  int countNeg( java.lang.Double[] values ) {return DoubleNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#countNeg(java.lang.Float[]) */
    public static  int countNeg( java.lang.Float[] values ) {return FloatNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#countNeg(java.lang.Integer[]) */
    public static  int countNeg( java.lang.Integer[] values ) {return IntegerNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#countNeg(java.lang.Long[]) */
    public static  int countNeg( java.lang.Long[] values ) {return LongNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#countNeg(java.lang.Short[]) */
    public static  int countNeg( java.lang.Short[] values ) {return ShortNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#countNeg(short[]) */
    public static  int countNeg( short[] values ) {return ShortNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#countNeg(io.deephaven.vector.ByteVector) */
    public static  int countNeg( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#countNeg(io.deephaven.vector.DoubleVector) */
    public static  int countNeg( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#countNeg(io.deephaven.vector.FloatVector) */
    public static  int countNeg( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#countNeg(io.deephaven.vector.IntVector) */
    public static  int countNeg( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#countNeg(io.deephaven.vector.LongVector) */
    public static  int countNeg( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#countNeg(io.deephaven.vector.ShortVector) */
    public static  int countNeg( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.countNeg( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#countPos(byte[]) */
    public static  int countPos( byte[] values ) {return ByteNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#countPos(double[]) */
    public static  int countPos( double[] values ) {return DoubleNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#countPos(float[]) */
    public static  int countPos( float[] values ) {return FloatNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#countPos(int[]) */
    public static  int countPos( int[] values ) {return IntegerNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#countPos(long[]) */
    public static  int countPos( long[] values ) {return LongNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#countPos(java.lang.Byte[]) */
    public static  int countPos( java.lang.Byte[] values ) {return ByteNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#countPos(java.lang.Double[]) */
    public static  int countPos( java.lang.Double[] values ) {return DoubleNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#countPos(java.lang.Float[]) */
    public static  int countPos( java.lang.Float[] values ) {return FloatNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#countPos(java.lang.Integer[]) */
    public static  int countPos( java.lang.Integer[] values ) {return IntegerNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#countPos(java.lang.Long[]) */
    public static  int countPos( java.lang.Long[] values ) {return LongNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#countPos(java.lang.Short[]) */
    public static  int countPos( java.lang.Short[] values ) {return ShortNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#countPos(short[]) */
    public static  int countPos( short[] values ) {return ShortNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#countPos(io.deephaven.vector.ByteVector) */
    public static  int countPos( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#countPos(io.deephaven.vector.DoubleVector) */
    public static  int countPos( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#countPos(io.deephaven.vector.FloatVector) */
    public static  int countPos( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#countPos(io.deephaven.vector.IntVector) */
    public static  int countPos( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#countPos(io.deephaven.vector.LongVector) */
    public static  int countPos( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#countPos(io.deephaven.vector.ShortVector) */
    public static  int countPos( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.countPos( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#countZero(byte[]) */
    public static  int countZero( byte[] values ) {return ByteNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#countZero(double[]) */
    public static  int countZero( double[] values ) {return DoubleNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#countZero(float[]) */
    public static  int countZero( float[] values ) {return FloatNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#countZero(int[]) */
    public static  int countZero( int[] values ) {return IntegerNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#countZero(long[]) */
    public static  int countZero( long[] values ) {return LongNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#countZero(java.lang.Byte[]) */
    public static  int countZero( java.lang.Byte[] values ) {return ByteNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#countZero(java.lang.Double[]) */
    public static  int countZero( java.lang.Double[] values ) {return DoubleNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#countZero(java.lang.Float[]) */
    public static  int countZero( java.lang.Float[] values ) {return FloatNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#countZero(java.lang.Integer[]) */
    public static  int countZero( java.lang.Integer[] values ) {return IntegerNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#countZero(java.lang.Long[]) */
    public static  int countZero( java.lang.Long[] values ) {return LongNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#countZero(java.lang.Short[]) */
    public static  int countZero( java.lang.Short[] values ) {return ShortNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#countZero(short[]) */
    public static  int countZero( short[] values ) {return ShortNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#countZero(io.deephaven.vector.ByteVector) */
    public static  int countZero( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#countZero(io.deephaven.vector.DoubleVector) */
    public static  int countZero( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#countZero(io.deephaven.vector.FloatVector) */
    public static  int countZero( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#countZero(io.deephaven.vector.IntVector) */
    public static  int countZero( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#countZero(io.deephaven.vector.LongVector) */
    public static  int countZero( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#countZero(io.deephaven.vector.ShortVector) */
    public static  int countZero( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.countZero( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cov(byte[],byte[]) */
    public static  double cov( byte[] values0, byte[] values1 ) {return ByteNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cov(byte[],io.deephaven.vector.ByteVector) */
    public static  double cov( byte[] values0, io.deephaven.vector.ByteVector values1 ) {return ByteNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cov(double[],double[]) */
    public static  double cov( double[] values0, double[] values1 ) {return DoubleNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cov(double[],io.deephaven.vector.DoubleVector) */
    public static  double cov( double[] values0, io.deephaven.vector.DoubleVector values1 ) {return DoubleNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cov(float[],float[]) */
    public static  double cov( float[] values0, float[] values1 ) {return FloatNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cov(float[],io.deephaven.vector.FloatVector) */
    public static  double cov( float[] values0, io.deephaven.vector.FloatVector values1 ) {return FloatNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cov(int[],int[]) */
    public static  double cov( int[] values0, int[] values1 ) {return IntegerNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cov(int[],io.deephaven.vector.IntVector) */
    public static  double cov( int[] values0, io.deephaven.vector.IntVector values1 ) {return IntegerNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.LongNumericPrimitives#cov(long[],long[]) */
    public static  double cov( long[] values0, long[] values1 ) {return LongNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.LongNumericPrimitives#cov(long[],io.deephaven.vector.LongVector) */
    public static  double cov( long[] values0, io.deephaven.vector.LongVector values1 ) {return LongNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cov(short[],short[]) */
    public static  double cov( short[] values0, short[] values1 ) {return ShortNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cov(short[],io.deephaven.vector.ShortVector) */
    public static  double cov( short[] values0, io.deephaven.vector.ShortVector values1 ) {return ShortNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cov(io.deephaven.vector.ByteVector,byte[]) */
    public static  double cov( io.deephaven.vector.ByteVector values0, byte[] values1 ) {return ByteNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cov(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double cov( io.deephaven.vector.ByteVector values0, io.deephaven.vector.ByteVector values1 ) {return ByteNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cov(io.deephaven.vector.DoubleVector,double[]) */
    public static  double cov( io.deephaven.vector.DoubleVector values0, double[] values1 ) {return DoubleNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cov(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double cov( io.deephaven.vector.DoubleVector values0, io.deephaven.vector.DoubleVector values1 ) {return DoubleNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cov(io.deephaven.vector.FloatVector,float[]) */
    public static  double cov( io.deephaven.vector.FloatVector values0, float[] values1 ) {return FloatNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cov(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double cov( io.deephaven.vector.FloatVector values0, io.deephaven.vector.FloatVector values1 ) {return FloatNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cov(io.deephaven.vector.IntVector,int[]) */
    public static  double cov( io.deephaven.vector.IntVector values0, int[] values1 ) {return IntegerNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cov(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double cov( io.deephaven.vector.IntVector values0, io.deephaven.vector.IntVector values1 ) {return IntegerNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.LongNumericPrimitives#cov(io.deephaven.vector.LongVector,long[]) */
    public static  double cov( io.deephaven.vector.LongVector values0, long[] values1 ) {return LongNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.LongNumericPrimitives#cov(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double cov( io.deephaven.vector.LongVector values0, io.deephaven.vector.LongVector values1 ) {return LongNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cov(io.deephaven.vector.ShortVector,short[]) */
    public static  double cov( io.deephaven.vector.ShortVector values0, short[] values1 ) {return ShortNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cov(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double cov( io.deephaven.vector.ShortVector values0, io.deephaven.vector.ShortVector values1 ) {return ShortNumericPrimitives.cov( values0, values1 );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cumprod(byte[]) */
    public static  byte[] cumprod( byte[] values ) {return ByteNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cumprod(double[]) */
    public static  double[] cumprod( double[] values ) {return DoubleNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cumprod(float[]) */
    public static  float[] cumprod( float[] values ) {return FloatNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cumprod(int[]) */
    public static  int[] cumprod( int[] values ) {return IntegerNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#cumprod(long[]) */
    public static  long[] cumprod( long[] values ) {return LongNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cumprod(java.lang.Byte[]) */
    public static  byte[] cumprod( java.lang.Byte[] values ) {return ByteNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cumprod(java.lang.Double[]) */
    public static  double[] cumprod( java.lang.Double[] values ) {return DoubleNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cumprod(java.lang.Float[]) */
    public static  float[] cumprod( java.lang.Float[] values ) {return FloatNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cumprod(java.lang.Integer[]) */
    public static  int[] cumprod( java.lang.Integer[] values ) {return IntegerNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#cumprod(java.lang.Long[]) */
    public static  long[] cumprod( java.lang.Long[] values ) {return LongNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cumprod(java.lang.Short[]) */
    public static  short[] cumprod( java.lang.Short[] values ) {return ShortNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cumprod(short[]) */
    public static  short[] cumprod( short[] values ) {return ShortNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cumprod(io.deephaven.vector.ByteVector) */
    public static  byte[] cumprod( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cumprod(io.deephaven.vector.DoubleVector) */
    public static  double[] cumprod( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cumprod(io.deephaven.vector.FloatVector) */
    public static  float[] cumprod( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cumprod(io.deephaven.vector.IntVector) */
    public static  int[] cumprod( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#cumprod(io.deephaven.vector.LongVector) */
    public static  long[] cumprod( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cumprod(io.deephaven.vector.ShortVector) */
    public static  short[] cumprod( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.cumprod( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cumsum(byte[]) */
    public static  byte[] cumsum( byte[] values ) {return ByteNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cumsum(double[]) */
    public static  double[] cumsum( double[] values ) {return DoubleNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cumsum(float[]) */
    public static  float[] cumsum( float[] values ) {return FloatNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cumsum(int[]) */
    public static  int[] cumsum( int[] values ) {return IntegerNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#cumsum(long[]) */
    public static  long[] cumsum( long[] values ) {return LongNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cumsum(java.lang.Byte[]) */
    public static  byte[] cumsum( java.lang.Byte[] values ) {return ByteNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cumsum(java.lang.Double[]) */
    public static  double[] cumsum( java.lang.Double[] values ) {return DoubleNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cumsum(java.lang.Float[]) */
    public static  float[] cumsum( java.lang.Float[] values ) {return FloatNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cumsum(java.lang.Integer[]) */
    public static  int[] cumsum( java.lang.Integer[] values ) {return IntegerNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#cumsum(java.lang.Long[]) */
    public static  long[] cumsum( java.lang.Long[] values ) {return LongNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cumsum(java.lang.Short[]) */
    public static  short[] cumsum( java.lang.Short[] values ) {return ShortNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cumsum(short[]) */
    public static  short[] cumsum( short[] values ) {return ShortNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#cumsum(io.deephaven.vector.ByteVector) */
    public static  byte[] cumsum( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#cumsum(io.deephaven.vector.DoubleVector) */
    public static  double[] cumsum( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#cumsum(io.deephaven.vector.FloatVector) */
    public static  float[] cumsum( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#cumsum(io.deephaven.vector.IntVector) */
    public static  int[] cumsum( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#cumsum(io.deephaven.vector.LongVector) */
    public static  long[] cumsum( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#cumsum(io.deephaven.vector.ShortVector) */
    public static  short[] cumsum( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.cumsum( values );}
    /** @see io.deephaven.function.BytePrimitives#distinct(byte[]) */
    public static  byte[] distinct( byte[] values ) {return BytePrimitives.distinct( values );}
    /** @see io.deephaven.function.CharacterPrimitives#distinct(char[]) */
    public static  char[] distinct( char[] values ) {return CharacterPrimitives.distinct( values );}
    /** @see io.deephaven.function.DoublePrimitives#distinct(double[]) */
    public static  double[] distinct( double[] values ) {return DoublePrimitives.distinct( values );}
    /** @see io.deephaven.function.FloatPrimitives#distinct(float[]) */
    public static  float[] distinct( float[] values ) {return FloatPrimitives.distinct( values );}
    /** @see io.deephaven.function.IntegerPrimitives#distinct(int[]) */
    public static  int[] distinct( int[] values ) {return IntegerPrimitives.distinct( values );}
    /** @see io.deephaven.function.LongPrimitives#distinct(long[]) */
    public static  long[] distinct( long[] values ) {return LongPrimitives.distinct( values );}
    /** @see io.deephaven.function.ShortPrimitives#distinct(short[]) */
    public static  short[] distinct( short[] values ) {return ShortPrimitives.distinct( values );}
    /** @see io.deephaven.function.BytePrimitives#distinct(io.deephaven.vector.ByteVector) */
    public static  io.deephaven.vector.ByteVector distinct( io.deephaven.vector.ByteVector values ) {return BytePrimitives.distinct( values );}
    /** @see io.deephaven.function.CharacterPrimitives#distinct(io.deephaven.vector.CharVector) */
    public static  io.deephaven.vector.CharVector distinct( io.deephaven.vector.CharVector values ) {return CharacterPrimitives.distinct( values );}
    /** @see io.deephaven.function.DoublePrimitives#distinct(io.deephaven.vector.DoubleVector) */
    public static  io.deephaven.vector.DoubleVector distinct( io.deephaven.vector.DoubleVector values ) {return DoublePrimitives.distinct( values );}
    /** @see io.deephaven.function.FloatPrimitives#distinct(io.deephaven.vector.FloatVector) */
    public static  io.deephaven.vector.FloatVector distinct( io.deephaven.vector.FloatVector values ) {return FloatPrimitives.distinct( values );}
    /** @see io.deephaven.function.IntegerPrimitives#distinct(io.deephaven.vector.IntVector) */
    public static  io.deephaven.vector.IntVector distinct( io.deephaven.vector.IntVector values ) {return IntegerPrimitives.distinct( values );}
    /** @see io.deephaven.function.LongPrimitives#distinct(io.deephaven.vector.LongVector) */
    public static  io.deephaven.vector.LongVector distinct( io.deephaven.vector.LongVector values ) {return LongPrimitives.distinct( values );}
    /** @see io.deephaven.function.ShortPrimitives#distinct(io.deephaven.vector.ShortVector) */
    public static  io.deephaven.vector.ShortVector distinct( io.deephaven.vector.ShortVector values ) {return ShortPrimitives.distinct( values );}
    /** @see io.deephaven.function.ObjectPrimitives#distinct(io.deephaven.vector.ObjectVector) */
    public static <T extends java.lang.Comparable<? super T>> io.deephaven.vector.ObjectVector<T> distinct( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.distinct( values );}
    /** @see io.deephaven.function.BytePrimitives#distinct(byte[],boolean,boolean) */
    public static  byte[] distinct( byte[] values, boolean includeNull, boolean sort ) {return BytePrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.CharacterPrimitives#distinct(char[],boolean,boolean) */
    public static  char[] distinct( char[] values, boolean includeNull, boolean sort ) {return CharacterPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.DoublePrimitives#distinct(double[],boolean,boolean) */
    public static  double[] distinct( double[] values, boolean includeNull, boolean sort ) {return DoublePrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.FloatPrimitives#distinct(float[],boolean,boolean) */
    public static  float[] distinct( float[] values, boolean includeNull, boolean sort ) {return FloatPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.IntegerPrimitives#distinct(int[],boolean,boolean) */
    public static  int[] distinct( int[] values, boolean includeNull, boolean sort ) {return IntegerPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.LongPrimitives#distinct(long[],boolean,boolean) */
    public static  long[] distinct( long[] values, boolean includeNull, boolean sort ) {return LongPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.ShortPrimitives#distinct(short[],boolean,boolean) */
    public static  short[] distinct( short[] values, boolean includeNull, boolean sort ) {return ShortPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.BytePrimitives#distinct(io.deephaven.vector.ByteVector,boolean,boolean) */
    public static  io.deephaven.vector.ByteVector distinct( io.deephaven.vector.ByteVector values, boolean includeNull, boolean sort ) {return BytePrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.CharacterPrimitives#distinct(io.deephaven.vector.CharVector,boolean,boolean) */
    public static  io.deephaven.vector.CharVector distinct( io.deephaven.vector.CharVector values, boolean includeNull, boolean sort ) {return CharacterPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.DoublePrimitives#distinct(io.deephaven.vector.DoubleVector,boolean,boolean) */
    public static  io.deephaven.vector.DoubleVector distinct( io.deephaven.vector.DoubleVector values, boolean includeNull, boolean sort ) {return DoublePrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.FloatPrimitives#distinct(io.deephaven.vector.FloatVector,boolean,boolean) */
    public static  io.deephaven.vector.FloatVector distinct( io.deephaven.vector.FloatVector values, boolean includeNull, boolean sort ) {return FloatPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.IntegerPrimitives#distinct(io.deephaven.vector.IntVector,boolean,boolean) */
    public static  io.deephaven.vector.IntVector distinct( io.deephaven.vector.IntVector values, boolean includeNull, boolean sort ) {return IntegerPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.LongPrimitives#distinct(io.deephaven.vector.LongVector,boolean,boolean) */
    public static  io.deephaven.vector.LongVector distinct( io.deephaven.vector.LongVector values, boolean includeNull, boolean sort ) {return LongPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.ShortPrimitives#distinct(io.deephaven.vector.ShortVector,boolean,boolean) */
    public static  io.deephaven.vector.ShortVector distinct( io.deephaven.vector.ShortVector values, boolean includeNull, boolean sort ) {return ShortPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.ObjectPrimitives#distinct(io.deephaven.vector.ObjectVector,boolean,boolean) */
    public static <T extends java.lang.Comparable<? super T>> io.deephaven.vector.ObjectVector<T> distinct( io.deephaven.vector.ObjectVector<T> values, boolean includeNull, boolean sort ) {return ObjectPrimitives.distinct( values, includeNull, sort );}
    /** @see io.deephaven.function.BytePrimitives#enlist(byte[]) */
    public static  byte[] enlist( byte[] values ) {return BytePrimitives.enlist( values );}
    /** @see io.deephaven.function.CharacterPrimitives#enlist(char[]) */
    public static  char[] enlist( char[] values ) {return CharacterPrimitives.enlist( values );}
    /** @see io.deephaven.function.DoublePrimitives#enlist(double[]) */
    public static  double[] enlist( double[] values ) {return DoublePrimitives.enlist( values );}
    /** @see io.deephaven.function.FloatPrimitives#enlist(float[]) */
    public static  float[] enlist( float[] values ) {return FloatPrimitives.enlist( values );}
    /** @see io.deephaven.function.IntegerPrimitives#enlist(int[]) */
    public static  int[] enlist( int[] values ) {return IntegerPrimitives.enlist( values );}
    /** @see io.deephaven.function.LongPrimitives#enlist(long[]) */
    public static  long[] enlist( long[] values ) {return LongPrimitives.enlist( values );}
    /** @see io.deephaven.function.ShortPrimitives#enlist(short[]) */
    public static  short[] enlist( short[] values ) {return ShortPrimitives.enlist( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#exp(byte) */
    public static  double exp( byte value ) {return ByteNumericPrimitives.exp( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#exp(double) */
    public static  double exp( double value ) {return DoubleNumericPrimitives.exp( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#exp(float) */
    public static  double exp( float value ) {return FloatNumericPrimitives.exp( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#exp(int) */
    public static  double exp( int value ) {return IntegerNumericPrimitives.exp( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#exp(long) */
    public static  double exp( long value ) {return LongNumericPrimitives.exp( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#exp(short) */
    public static  double exp( short value ) {return ShortNumericPrimitives.exp( value );}
    /** @see io.deephaven.function.BytePrimitives#first(byte[]) */
    public static  byte first( byte[] values ) {return BytePrimitives.first( values );}
    /** @see io.deephaven.function.CharacterPrimitives#first(char[]) */
    public static  char first( char[] values ) {return CharacterPrimitives.first( values );}
    /** @see io.deephaven.function.DoublePrimitives#first(double[]) */
    public static  double first( double[] values ) {return DoublePrimitives.first( values );}
    /** @see io.deephaven.function.FloatPrimitives#first(float[]) */
    public static  float first( float[] values ) {return FloatPrimitives.first( values );}
    /** @see io.deephaven.function.IntegerPrimitives#first(int[]) */
    public static  int first( int[] values ) {return IntegerPrimitives.first( values );}
    /** @see io.deephaven.function.LongPrimitives#first(long[]) */
    public static  long first( long[] values ) {return LongPrimitives.first( values );}
    /** @see io.deephaven.function.BooleanPrimitives#first(java.lang.Boolean[]) */
    public static  java.lang.Boolean first( java.lang.Boolean[] values ) {return BooleanPrimitives.first( values );}
    /** @see io.deephaven.function.ShortPrimitives#first(short[]) */
    public static  short first( short[] values ) {return ShortPrimitives.first( values );}
    /** @see io.deephaven.function.BooleanPrimitives#first(boolean[]) */
    public static  java.lang.Boolean first( boolean[] values ) {return BooleanPrimitives.first( values );}
    /** @see io.deephaven.function.BytePrimitives#first(io.deephaven.vector.ByteVector) */
    public static  byte first( io.deephaven.vector.ByteVector values ) {return BytePrimitives.first( values );}
    /** @see io.deephaven.function.CharacterPrimitives#first(io.deephaven.vector.CharVector) */
    public static  char first( io.deephaven.vector.CharVector values ) {return CharacterPrimitives.first( values );}
    /** @see io.deephaven.function.DoublePrimitives#first(io.deephaven.vector.DoubleVector) */
    public static  double first( io.deephaven.vector.DoubleVector values ) {return DoublePrimitives.first( values );}
    /** @see io.deephaven.function.FloatPrimitives#first(io.deephaven.vector.FloatVector) */
    public static  float first( io.deephaven.vector.FloatVector values ) {return FloatPrimitives.first( values );}
    /** @see io.deephaven.function.IntegerPrimitives#first(io.deephaven.vector.IntVector) */
    public static  int first( io.deephaven.vector.IntVector values ) {return IntegerPrimitives.first( values );}
    /** @see io.deephaven.function.LongPrimitives#first(io.deephaven.vector.LongVector) */
    public static  long first( io.deephaven.vector.LongVector values ) {return LongPrimitives.first( values );}
    /** @see io.deephaven.function.ShortPrimitives#first(io.deephaven.vector.ShortVector) */
    public static  short first( io.deephaven.vector.ShortVector values ) {return ShortPrimitives.first( values );}
    /** @see io.deephaven.function.ObjectPrimitives#first(io.deephaven.vector.ObjectVector) */
    public static <T> T first( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.first( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#firstIndexOf(byte[],byte) */
    public static  int firstIndexOf( byte[] values, byte val ) {return ByteNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#firstIndexOf(double[],double) */
    public static  int firstIndexOf( double[] values, double val ) {return DoubleNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.FloatNumericPrimitives#firstIndexOf(float[],float) */
    public static  int firstIndexOf( float[] values, float val ) {return FloatNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#firstIndexOf(int[],int) */
    public static  int firstIndexOf( int[] values, int val ) {return IntegerNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.LongNumericPrimitives#firstIndexOf(long[],long) */
    public static  int firstIndexOf( long[] values, long val ) {return LongNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.ShortNumericPrimitives#firstIndexOf(short[],short) */
    public static  int firstIndexOf( short[] values, short val ) {return ShortNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.ByteNumericPrimitives#firstIndexOf(io.deephaven.vector.ByteVector,byte) */
    public static  int firstIndexOf( io.deephaven.vector.ByteVector values, byte val ) {return ByteNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#firstIndexOf(io.deephaven.vector.DoubleVector,double) */
    public static  int firstIndexOf( io.deephaven.vector.DoubleVector values, double val ) {return DoubleNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.FloatNumericPrimitives#firstIndexOf(io.deephaven.vector.FloatVector,float) */
    public static  int firstIndexOf( io.deephaven.vector.FloatVector values, float val ) {return FloatNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#firstIndexOf(io.deephaven.vector.IntVector,int) */
    public static  int firstIndexOf( io.deephaven.vector.IntVector values, int val ) {return IntegerNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.LongNumericPrimitives#firstIndexOf(io.deephaven.vector.LongVector,long) */
    public static  int firstIndexOf( io.deephaven.vector.LongVector values, long val ) {return LongNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.ShortNumericPrimitives#firstIndexOf(io.deephaven.vector.ShortVector,short) */
    public static  int firstIndexOf( io.deephaven.vector.ShortVector values, short val ) {return ShortNumericPrimitives.firstIndexOf( values, val );}
    /** @see io.deephaven.function.ByteNumericPrimitives#floor(byte) */
    public static  double floor( byte value ) {return ByteNumericPrimitives.floor( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#floor(double) */
    public static  double floor( double value ) {return DoubleNumericPrimitives.floor( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#floor(float) */
    public static  double floor( float value ) {return FloatNumericPrimitives.floor( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#floor(int) */
    public static  double floor( int value ) {return IntegerNumericPrimitives.floor( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#floor(long) */
    public static  double floor( long value ) {return LongNumericPrimitives.floor( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#floor(short) */
    public static  double floor( short value ) {return ShortNumericPrimitives.floor( value );}
    /** @see io.deephaven.function.ObjectPrimitives#in(T,T[]) */
    public static <T> boolean in( T testedValue, T[] possibleValues ) {return ObjectPrimitives.in( testedValue, possibleValues );}
    /** @see io.deephaven.function.BytePrimitives#in(byte,byte[]) */
    public static  boolean in( byte testedValues, byte[] possibleValues ) {return BytePrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.function.CharacterPrimitives#in(char,char[]) */
    public static  boolean in( char testedValues, char[] possibleValues ) {return CharacterPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.function.DoublePrimitives#in(double,double[]) */
    public static  boolean in( double testedValues, double[] possibleValues ) {return DoublePrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.function.FloatPrimitives#in(float,float[]) */
    public static  boolean in( float testedValues, float[] possibleValues ) {return FloatPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.function.IntegerPrimitives#in(int,int[]) */
    public static  boolean in( int testedValues, int[] possibleValues ) {return IntegerPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.function.LongPrimitives#in(long,long[]) */
    public static  boolean in( long testedValues, long[] possibleValues ) {return LongPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.function.ShortPrimitives#in(short,short[]) */
    public static  boolean in( short testedValues, short[] possibleValues ) {return ShortPrimitives.in( testedValues, possibleValues );}
    /** @see io.deephaven.function.BytePrimitives#inRange(byte,byte,byte) */
    public static  boolean inRange( byte testedValue, byte lowInclusiveValue, byte highInclusiveValue ) {return BytePrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.function.CharacterPrimitives#inRange(char,char,char) */
    public static  boolean inRange( char testedValue, char lowInclusiveValue, char highInclusiveValue ) {return CharacterPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.function.DoublePrimitives#inRange(double,double,double) */
    public static  boolean inRange( double testedValue, double lowInclusiveValue, double highInclusiveValue ) {return DoublePrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.function.FloatPrimitives#inRange(float,float,float) */
    public static  boolean inRange( float testedValue, float lowInclusiveValue, float highInclusiveValue ) {return FloatPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.function.IntegerPrimitives#inRange(int,int,int) */
    public static  boolean inRange( int testedValue, int lowInclusiveValue, int highInclusiveValue ) {return IntegerPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.function.ObjectPrimitives#inRange(java.lang.Comparable,java.lang.Comparable,java.lang.Comparable) */
    public static  boolean inRange( java.lang.Comparable testedValue, java.lang.Comparable lowInclusiveValue, java.lang.Comparable highInclusiveValue ) {return ObjectPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.function.LongPrimitives#inRange(long,long,long) */
    public static  boolean inRange( long testedValue, long lowInclusiveValue, long highInclusiveValue ) {return LongPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.function.ShortPrimitives#inRange(short,short,short) */
    public static  boolean inRange( short testedValue, short lowInclusiveValue, short highInclusiveValue ) {return ShortPrimitives.inRange( testedValue, lowInclusiveValue, highInclusiveValue );}
    /** @see io.deephaven.function.ByteNumericPrimitives#indexOfMax(byte[]) */
    public static  int indexOfMax( byte[] values ) {return ByteNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#indexOfMax(double[]) */
    public static  int indexOfMax( double[] values ) {return DoubleNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#indexOfMax(float[]) */
    public static  int indexOfMax( float[] values ) {return FloatNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#indexOfMax(int[]) */
    public static  int indexOfMax( int[] values ) {return IntegerNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#indexOfMax(long[]) */
    public static  int indexOfMax( long[] values ) {return LongNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#indexOfMax(java.lang.Byte[]) */
    public static  int indexOfMax( java.lang.Byte[] values ) {return ByteNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#indexOfMax(java.lang.Double[]) */
    public static  int indexOfMax( java.lang.Double[] values ) {return DoubleNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#indexOfMax(java.lang.Float[]) */
    public static  int indexOfMax( java.lang.Float[] values ) {return FloatNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#indexOfMax(java.lang.Integer[]) */
    public static  int indexOfMax( java.lang.Integer[] values ) {return IntegerNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#indexOfMax(java.lang.Long[]) */
    public static  int indexOfMax( java.lang.Long[] values ) {return LongNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#indexOfMax(java.lang.Short[]) */
    public static  int indexOfMax( java.lang.Short[] values ) {return ShortNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#indexOfMax(short[]) */
    public static  int indexOfMax( short[] values ) {return ShortNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#indexOfMax(io.deephaven.vector.ByteVector) */
    public static  int indexOfMax( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#indexOfMax(io.deephaven.vector.DoubleVector) */
    public static  int indexOfMax( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#indexOfMax(io.deephaven.vector.FloatVector) */
    public static  int indexOfMax( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#indexOfMax(io.deephaven.vector.IntVector) */
    public static  int indexOfMax( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#indexOfMax(io.deephaven.vector.LongVector) */
    public static  int indexOfMax( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#indexOfMax(io.deephaven.vector.ShortVector) */
    public static  int indexOfMax( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.indexOfMax( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#indexOfMin(byte[]) */
    public static  int indexOfMin( byte[] values ) {return ByteNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#indexOfMin(double[]) */
    public static  int indexOfMin( double[] values ) {return DoubleNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#indexOfMin(float[]) */
    public static  int indexOfMin( float[] values ) {return FloatNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#indexOfMin(int[]) */
    public static  int indexOfMin( int[] values ) {return IntegerNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#indexOfMin(long[]) */
    public static  int indexOfMin( long[] values ) {return LongNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#indexOfMin(java.lang.Byte[]) */
    public static  int indexOfMin( java.lang.Byte[] values ) {return ByteNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#indexOfMin(java.lang.Double[]) */
    public static  int indexOfMin( java.lang.Double[] values ) {return DoubleNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#indexOfMin(java.lang.Float[]) */
    public static  int indexOfMin( java.lang.Float[] values ) {return FloatNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#indexOfMin(java.lang.Integer[]) */
    public static  int indexOfMin( java.lang.Integer[] values ) {return IntegerNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#indexOfMin(java.lang.Long[]) */
    public static  int indexOfMin( java.lang.Long[] values ) {return LongNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#indexOfMin(java.lang.Short[]) */
    public static  int indexOfMin( java.lang.Short[] values ) {return ShortNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#indexOfMin(short[]) */
    public static  int indexOfMin( short[] values ) {return ShortNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#indexOfMin(io.deephaven.vector.ByteVector) */
    public static  int indexOfMin( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#indexOfMin(io.deephaven.vector.DoubleVector) */
    public static  int indexOfMin( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#indexOfMin(io.deephaven.vector.FloatVector) */
    public static  int indexOfMin( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#indexOfMin(io.deephaven.vector.IntVector) */
    public static  int indexOfMin( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#indexOfMin(io.deephaven.vector.LongVector) */
    public static  int indexOfMin( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#indexOfMin(io.deephaven.vector.ShortVector) */
    public static  int indexOfMin( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.indexOfMin( values );}
    /** @see io.deephaven.function.Casting#intToDouble(int[]) */
    public static  double[] intToDouble( int[] values ) {return Casting.intToDouble( values );}
    /** @see io.deephaven.function.Casting#intToDouble(io.deephaven.vector.IntVector) */
    public static  io.deephaven.vector.DoubleVector intToDouble( io.deephaven.vector.IntVector values ) {return Casting.intToDouble( values );}
    /** @see io.deephaven.function.ObjectPrimitives#isDBNull(T) */
    public static <T> boolean isDBNull( T value ) {return ObjectPrimitives.isDBNull( value );}
    /** @see io.deephaven.function.DoubleFpPrimitives#isInf(double) */
    public static  boolean isInf( double value ) {return DoubleFpPrimitives.isInf( value );}
    /** @see io.deephaven.function.FloatFpPrimitives#isInf(float) */
    public static  boolean isInf( float value ) {return FloatFpPrimitives.isInf( value );}
    /** @see io.deephaven.function.DoubleFpPrimitives#isNaN(double) */
    public static  boolean isNaN( double value ) {return DoubleFpPrimitives.isNaN( value );}
    /** @see io.deephaven.function.FloatFpPrimitives#isNaN(float) */
    public static  boolean isNaN( float value ) {return FloatFpPrimitives.isNaN( value );}
    /** @see io.deephaven.function.DoubleFpPrimitives#isNormal(double) */
    public static  boolean isNormal( double value ) {return DoubleFpPrimitives.isNormal( value );}
    /** @see io.deephaven.function.FloatFpPrimitives#isNormal(float) */
    public static  boolean isNormal( float value ) {return FloatFpPrimitives.isNormal( value );}
    /** @see io.deephaven.function.ObjectPrimitives#isNull(T) */
    public static <T> boolean isNull( T value ) {return ObjectPrimitives.isNull( value );}
    /** @see io.deephaven.function.BytePrimitives#isNull(byte) */
    public static  boolean isNull( byte value ) {return BytePrimitives.isNull( value );}
    /** @see io.deephaven.function.CharacterPrimitives#isNull(char) */
    public static  boolean isNull( char value ) {return CharacterPrimitives.isNull( value );}
    /** @see io.deephaven.function.BooleanPrimitives#isNull(java.lang.Boolean) */
    public static  boolean isNull( java.lang.Boolean value ) {return BooleanPrimitives.isNull( value );}
    /** @see io.deephaven.function.DoublePrimitives#isNull(double) */
    public static  boolean isNull( double value ) {return DoublePrimitives.isNull( value );}
    /** @see io.deephaven.function.FloatPrimitives#isNull(float) */
    public static  boolean isNull( float value ) {return FloatPrimitives.isNull( value );}
    /** @see io.deephaven.function.IntegerPrimitives#isNull(int) */
    public static  boolean isNull( int value ) {return IntegerPrimitives.isNull( value );}
    /** @see io.deephaven.function.LongPrimitives#isNull(long) */
    public static  boolean isNull( long value ) {return LongPrimitives.isNull( value );}
    /** @see io.deephaven.function.ShortPrimitives#isNull(short) */
    public static  boolean isNull( short value ) {return ShortPrimitives.isNull( value );}
    /** @see io.deephaven.function.BytePrimitives#last(byte[]) */
    public static  byte last( byte[] values ) {return BytePrimitives.last( values );}
    /** @see io.deephaven.function.CharacterPrimitives#last(char[]) */
    public static  char last( char[] values ) {return CharacterPrimitives.last( values );}
    /** @see io.deephaven.function.DoublePrimitives#last(double[]) */
    public static  double last( double[] values ) {return DoublePrimitives.last( values );}
    /** @see io.deephaven.function.FloatPrimitives#last(float[]) */
    public static  float last( float[] values ) {return FloatPrimitives.last( values );}
    /** @see io.deephaven.function.IntegerPrimitives#last(int[]) */
    public static  int last( int[] values ) {return IntegerPrimitives.last( values );}
    /** @see io.deephaven.function.LongPrimitives#last(long[]) */
    public static  long last( long[] values ) {return LongPrimitives.last( values );}
    /** @see io.deephaven.function.BooleanPrimitives#last(java.lang.Boolean[]) */
    public static  java.lang.Boolean last( java.lang.Boolean[] values ) {return BooleanPrimitives.last( values );}
    /** @see io.deephaven.function.ShortPrimitives#last(short[]) */
    public static  short last( short[] values ) {return ShortPrimitives.last( values );}
    /** @see io.deephaven.function.BooleanPrimitives#last(boolean[]) */
    public static  java.lang.Boolean last( boolean[] values ) {return BooleanPrimitives.last( values );}
    /** @see io.deephaven.function.BytePrimitives#last(io.deephaven.vector.ByteVector) */
    public static  byte last( io.deephaven.vector.ByteVector values ) {return BytePrimitives.last( values );}
    /** @see io.deephaven.function.CharacterPrimitives#last(io.deephaven.vector.CharVector) */
    public static  char last( io.deephaven.vector.CharVector values ) {return CharacterPrimitives.last( values );}
    /** @see io.deephaven.function.DoublePrimitives#last(io.deephaven.vector.DoubleVector) */
    public static  double last( io.deephaven.vector.DoubleVector values ) {return DoublePrimitives.last( values );}
    /** @see io.deephaven.function.FloatPrimitives#last(io.deephaven.vector.FloatVector) */
    public static  float last( io.deephaven.vector.FloatVector values ) {return FloatPrimitives.last( values );}
    /** @see io.deephaven.function.IntegerPrimitives#last(io.deephaven.vector.IntVector) */
    public static  int last( io.deephaven.vector.IntVector values ) {return IntegerPrimitives.last( values );}
    /** @see io.deephaven.function.LongPrimitives#last(io.deephaven.vector.LongVector) */
    public static  long last( io.deephaven.vector.LongVector values ) {return LongPrimitives.last( values );}
    /** @see io.deephaven.function.ShortPrimitives#last(io.deephaven.vector.ShortVector) */
    public static  short last( io.deephaven.vector.ShortVector values ) {return ShortPrimitives.last( values );}
    /** @see io.deephaven.function.ObjectPrimitives#last(io.deephaven.vector.ObjectVector) */
    public static <T> T last( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.last( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#log(byte) */
    public static  double log( byte value ) {return ByteNumericPrimitives.log( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#log(double) */
    public static  double log( double value ) {return DoubleNumericPrimitives.log( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#log(float) */
    public static  double log( float value ) {return FloatNumericPrimitives.log( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#log(int) */
    public static  double log( int value ) {return IntegerNumericPrimitives.log( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#log(long) */
    public static  double log( long value ) {return LongNumericPrimitives.log( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#log(short) */
    public static  double log( short value ) {return ShortNumericPrimitives.log( value );}
    /** @see io.deephaven.function.Casting#longToDouble(long[]) */
    public static  double[] longToDouble( long[] values ) {return Casting.longToDouble( values );}
    /** @see io.deephaven.function.Casting#longToDouble(io.deephaven.vector.LongVector) */
    public static  io.deephaven.vector.DoubleVector longToDouble( io.deephaven.vector.LongVector values ) {return Casting.longToDouble( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#lowerBin(byte,byte) */
    public static  byte lowerBin( byte value, byte interval ) {return ByteNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#lowerBin(double,double) */
    public static  double lowerBin( double value, double interval ) {return DoubleNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.function.FloatNumericPrimitives#lowerBin(float,float) */
    public static  float lowerBin( float value, float interval ) {return FloatNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#lowerBin(int,int) */
    public static  int lowerBin( int value, int interval ) {return IntegerNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.function.LongNumericPrimitives#lowerBin(long,long) */
    public static  long lowerBin( long value, long interval ) {return LongNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.function.ShortNumericPrimitives#lowerBin(short,short) */
    public static  short lowerBin( short value, short interval ) {return ShortNumericPrimitives.lowerBin( value, interval );}
    /** @see io.deephaven.function.ByteNumericPrimitives#lowerBin(byte,byte,byte) */
    public static  byte lowerBin( byte value, byte interval, byte offset ) {return ByteNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#lowerBin(double,double,double) */
    public static  double lowerBin( double value, double interval, double offset ) {return DoubleNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.function.FloatNumericPrimitives#lowerBin(float,float,float) */
    public static  float lowerBin( float value, float interval, float offset ) {return FloatNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#lowerBin(int,int,int) */
    public static  int lowerBin( int value, int interval, int offset ) {return IntegerNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.function.LongNumericPrimitives#lowerBin(long,long,long) */
    public static  long lowerBin( long value, long interval, long offset ) {return LongNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.function.ShortNumericPrimitives#lowerBin(short,short,short) */
    public static  short lowerBin( short value, short interval, short offset ) {return ShortNumericPrimitives.lowerBin( value, interval, offset );}
    /** @see io.deephaven.function.ObjectPrimitives#max(NUM[]) */
    public static <NUM extends java.lang.Number> NUM max( NUM[] values ) {return ObjectPrimitives.max( values );}
    /** @see io.deephaven.function.ObjectPrimitives#max(T[]) */
    public static <T> T max( T[] values ) {return ObjectPrimitives.max( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#max(byte[]) */
    public static  byte max( byte[] values ) {return ByteNumericPrimitives.max( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#max(double[]) */
    public static  double max( double[] values ) {return DoubleNumericPrimitives.max( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#max(float[]) */
    public static  float max( float[] values ) {return FloatNumericPrimitives.max( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#max(int[]) */
    public static  int max( int[] values ) {return IntegerNumericPrimitives.max( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#max(long[]) */
    public static  long max( long[] values ) {return LongNumericPrimitives.max( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#max(java.lang.Byte[]) */
    public static  byte max( java.lang.Byte[] values ) {return ByteNumericPrimitives.max( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#max(java.lang.Double[]) */
    public static  double max( java.lang.Double[] values ) {return DoubleNumericPrimitives.max( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#max(java.lang.Float[]) */
    public static  float max( java.lang.Float[] values ) {return FloatNumericPrimitives.max( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#max(java.lang.Integer[]) */
    public static  int max( java.lang.Integer[] values ) {return IntegerNumericPrimitives.max( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#max(java.lang.Long[]) */
    public static  long max( java.lang.Long[] values ) {return LongNumericPrimitives.max( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#max(java.lang.Short[]) */
    public static  short max( java.lang.Short[] values ) {return ShortNumericPrimitives.max( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#max(short[]) */
    public static  short max( short[] values ) {return ShortNumericPrimitives.max( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#max(io.deephaven.vector.ByteVector) */
    public static  byte max( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.max( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#max(io.deephaven.vector.DoubleVector) */
    public static  double max( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.max( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#max(io.deephaven.vector.FloatVector) */
    public static  float max( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.max( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#max(io.deephaven.vector.IntVector) */
    public static  int max( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.max( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#max(io.deephaven.vector.LongVector) */
    public static  long max( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.max( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#max(io.deephaven.vector.ShortVector) */
    public static  short max( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.max( values );}
    /** @see io.deephaven.function.ObjectPrimitives#max(io.deephaven.vector.ObjectVector) */
    public static <T extends java.lang.Comparable> T max( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.max( values );}
    /** @see io.deephaven.function.ComparePrimitives#max(byte,byte) */
    public static  byte max( byte v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(byte,double) */
    public static  double max( byte v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(byte,float) */
    public static  float max( byte v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(byte,int) */
    public static  int max( byte v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(byte,long) */
    public static  long max( byte v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(byte,short) */
    public static  short max( byte v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(double,byte) */
    public static  double max( double v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(double,double) */
    public static  double max( double v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(double,float) */
    public static  double max( double v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(double,int) */
    public static  double max( double v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(double,long) */
    public static  double max( double v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(double,short) */
    public static  double max( double v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(float,byte) */
    public static  float max( float v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(float,double) */
    public static  double max( float v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(float,float) */
    public static  float max( float v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(float,int) */
    public static  double max( float v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(float,long) */
    public static  double max( float v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(float,short) */
    public static  float max( float v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(int,byte) */
    public static  int max( int v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(int,double) */
    public static  double max( int v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(int,float) */
    public static  double max( int v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(int,int) */
    public static  int max( int v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(int,long) */
    public static  long max( int v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(int,short) */
    public static  int max( int v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(long,byte) */
    public static  long max( long v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(long,double) */
    public static  double max( long v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(long,float) */
    public static  double max( long v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(long,int) */
    public static  long max( long v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(long,long) */
    public static  long max( long v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(long,short) */
    public static  long max( long v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(short,byte) */
    public static  short max( short v1, byte v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(short,double) */
    public static  double max( short v1, double v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(short,float) */
    public static  float max( short v1, float v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(short,int) */
    public static  int max( short v1, int v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(short,long) */
    public static  long max( short v1, long v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#max(short,short) */
    public static  short max( short v1, short v2 ) {return ComparePrimitives.max( v1, v2 );}
    /** @see io.deephaven.function.ByteNumericPrimitives#median(byte[]) */
    public static  double median( byte[] values ) {return ByteNumericPrimitives.median( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#median(double[]) */
    public static  double median( double[] values ) {return DoubleNumericPrimitives.median( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#median(float[]) */
    public static  double median( float[] values ) {return FloatNumericPrimitives.median( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#median(int[]) */
    public static  double median( int[] values ) {return IntegerNumericPrimitives.median( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#median(long[]) */
    public static  double median( long[] values ) {return LongNumericPrimitives.median( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#median(java.lang.Byte[]) */
    public static  double median( java.lang.Byte[] values ) {return ByteNumericPrimitives.median( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#median(java.lang.Double[]) */
    public static  double median( java.lang.Double[] values ) {return DoubleNumericPrimitives.median( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#median(java.lang.Float[]) */
    public static  double median( java.lang.Float[] values ) {return FloatNumericPrimitives.median( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#median(java.lang.Integer[]) */
    public static  double median( java.lang.Integer[] values ) {return IntegerNumericPrimitives.median( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#median(java.lang.Long[]) */
    public static  double median( java.lang.Long[] values ) {return LongNumericPrimitives.median( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#median(java.lang.Short[]) */
    public static  double median( java.lang.Short[] values ) {return ShortNumericPrimitives.median( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#median(short[]) */
    public static  double median( short[] values ) {return ShortNumericPrimitives.median( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#median(io.deephaven.vector.ByteVector) */
    public static  double median( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.median( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#median(io.deephaven.vector.DoubleVector) */
    public static  double median( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.median( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#median(io.deephaven.vector.FloatVector) */
    public static  double median( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.median( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#median(io.deephaven.vector.IntVector) */
    public static  double median( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.median( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#median(io.deephaven.vector.LongVector) */
    public static  double median( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.median( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#median(io.deephaven.vector.ShortVector) */
    public static  double median( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.median( values );}
    /** @see io.deephaven.function.ObjectPrimitives#min(NUM[]) */
    public static <NUM extends java.lang.Number> NUM min( NUM[] values ) {return ObjectPrimitives.min( values );}
    /** @see io.deephaven.function.ObjectPrimitives#min(T[]) */
    public static <T> T min( T[] values ) {return ObjectPrimitives.min( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#min(byte[]) */
    public static  byte min( byte[] values ) {return ByteNumericPrimitives.min( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#min(double[]) */
    public static  double min( double[] values ) {return DoubleNumericPrimitives.min( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#min(float[]) */
    public static  float min( float[] values ) {return FloatNumericPrimitives.min( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#min(int[]) */
    public static  int min( int[] values ) {return IntegerNumericPrimitives.min( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#min(long[]) */
    public static  long min( long[] values ) {return LongNumericPrimitives.min( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#min(java.lang.Byte[]) */
    public static  byte min( java.lang.Byte[] values ) {return ByteNumericPrimitives.min( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#min(java.lang.Double[]) */
    public static  double min( java.lang.Double[] values ) {return DoubleNumericPrimitives.min( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#min(java.lang.Float[]) */
    public static  float min( java.lang.Float[] values ) {return FloatNumericPrimitives.min( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#min(java.lang.Integer[]) */
    public static  int min( java.lang.Integer[] values ) {return IntegerNumericPrimitives.min( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#min(java.lang.Long[]) */
    public static  long min( java.lang.Long[] values ) {return LongNumericPrimitives.min( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#min(java.lang.Short[]) */
    public static  short min( java.lang.Short[] values ) {return ShortNumericPrimitives.min( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#min(short[]) */
    public static  short min( short[] values ) {return ShortNumericPrimitives.min( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#min(io.deephaven.vector.ByteVector) */
    public static  byte min( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.min( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#min(io.deephaven.vector.DoubleVector) */
    public static  double min( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.min( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#min(io.deephaven.vector.FloatVector) */
    public static  float min( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.min( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#min(io.deephaven.vector.IntVector) */
    public static  int min( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.min( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#min(io.deephaven.vector.LongVector) */
    public static  long min( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.min( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#min(io.deephaven.vector.ShortVector) */
    public static  short min( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.min( values );}
    /** @see io.deephaven.function.ObjectPrimitives#min(io.deephaven.vector.ObjectVector) */
    public static <T extends java.lang.Comparable> T min( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.min( values );}
    /** @see io.deephaven.function.ComparePrimitives#min(byte,byte) */
    public static  byte min( byte v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(byte,double) */
    public static  double min( byte v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(byte,float) */
    public static  float min( byte v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(byte,int) */
    public static  int min( byte v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(byte,long) */
    public static  long min( byte v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(byte,short) */
    public static  short min( byte v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(double,byte) */
    public static  double min( double v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(double,double) */
    public static  double min( double v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(double,float) */
    public static  double min( double v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(double,int) */
    public static  double min( double v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(double,long) */
    public static  double min( double v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(double,short) */
    public static  double min( double v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(float,byte) */
    public static  float min( float v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(float,double) */
    public static  double min( float v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(float,float) */
    public static  float min( float v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(float,int) */
    public static  double min( float v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(float,long) */
    public static  double min( float v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(float,short) */
    public static  float min( float v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(int,byte) */
    public static  int min( int v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(int,double) */
    public static  double min( int v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(int,float) */
    public static  double min( int v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(int,int) */
    public static  int min( int v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(int,long) */
    public static  long min( int v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(int,short) */
    public static  int min( int v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(long,byte) */
    public static  long min( long v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(long,double) */
    public static  double min( long v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(long,float) */
    public static  double min( long v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(long,int) */
    public static  long min( long v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(long,long) */
    public static  long min( long v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(long,short) */
    public static  long min( long v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(short,byte) */
    public static  short min( short v1, byte v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(short,double) */
    public static  double min( short v1, double v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(short,float) */
    public static  float min( short v1, float v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(short,int) */
    public static  int min( short v1, int v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(short,long) */
    public static  long min( short v1, long v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.ComparePrimitives#min(short,short) */
    public static  short min( short v1, short v2 ) {return ComparePrimitives.min( v1, v2 );}
    /** @see io.deephaven.function.BooleanPrimitives#not(java.lang.Boolean[]) */
    public static  java.lang.Boolean[] not( java.lang.Boolean[] values ) {return BooleanPrimitives.not( values );}
    /** @see io.deephaven.function.BooleanPrimitives#not(boolean[]) */
    public static  java.lang.Boolean[] not( boolean[] values ) {return BooleanPrimitives.not( values );}
    /** @see io.deephaven.function.BytePrimitives#nth(int,byte[]) */
    public static  byte nth( int index, byte[] values ) {return BytePrimitives.nth( index, values );}
    /** @see io.deephaven.function.CharacterPrimitives#nth(int,char[]) */
    public static  char nth( int index, char[] values ) {return CharacterPrimitives.nth( index, values );}
    /** @see io.deephaven.function.DoublePrimitives#nth(int,double[]) */
    public static  double nth( int index, double[] values ) {return DoublePrimitives.nth( index, values );}
    /** @see io.deephaven.function.FloatPrimitives#nth(int,float[]) */
    public static  float nth( int index, float[] values ) {return FloatPrimitives.nth( index, values );}
    /** @see io.deephaven.function.IntegerPrimitives#nth(int,int[]) */
    public static  int nth( int index, int[] values ) {return IntegerPrimitives.nth( index, values );}
    /** @see io.deephaven.function.LongPrimitives#nth(int,long[]) */
    public static  long nth( int index, long[] values ) {return LongPrimitives.nth( index, values );}
    /** @see io.deephaven.function.BooleanPrimitives#nth(int,java.lang.Boolean[]) */
    public static  java.lang.Boolean nth( int index, java.lang.Boolean[] values ) {return BooleanPrimitives.nth( index, values );}
    /** @see io.deephaven.function.ShortPrimitives#nth(int,short[]) */
    public static  short nth( int index, short[] values ) {return ShortPrimitives.nth( index, values );}
    /** @see io.deephaven.function.BooleanPrimitives#nth(int,io.deephaven.vector.BooleanVector) */
    public static  java.lang.Boolean nth( int index, io.deephaven.vector.BooleanVector values ) {return BooleanPrimitives.nth( index, values );}
    /** @see io.deephaven.function.BytePrimitives#nth(int,io.deephaven.vector.ByteVector) */
    public static  byte nth( int index, io.deephaven.vector.ByteVector values ) {return BytePrimitives.nth( index, values );}
    /** @see io.deephaven.function.CharacterPrimitives#nth(int,io.deephaven.vector.CharVector) */
    public static  char nth( int index, io.deephaven.vector.CharVector values ) {return CharacterPrimitives.nth( index, values );}
    /** @see io.deephaven.function.DoublePrimitives#nth(int,io.deephaven.vector.DoubleVector) */
    public static  double nth( int index, io.deephaven.vector.DoubleVector values ) {return DoublePrimitives.nth( index, values );}
    /** @see io.deephaven.function.FloatPrimitives#nth(int,io.deephaven.vector.FloatVector) */
    public static  float nth( int index, io.deephaven.vector.FloatVector values ) {return FloatPrimitives.nth( index, values );}
    /** @see io.deephaven.function.IntegerPrimitives#nth(int,io.deephaven.vector.IntVector) */
    public static  int nth( int index, io.deephaven.vector.IntVector values ) {return IntegerPrimitives.nth( index, values );}
    /** @see io.deephaven.function.LongPrimitives#nth(int,io.deephaven.vector.LongVector) */
    public static  long nth( int index, io.deephaven.vector.LongVector values ) {return LongPrimitives.nth( index, values );}
    /** @see io.deephaven.function.ShortPrimitives#nth(int,io.deephaven.vector.ShortVector) */
    public static  short nth( int index, io.deephaven.vector.ShortVector values ) {return ShortPrimitives.nth( index, values );}
    /** @see io.deephaven.function.ObjectPrimitives#nth(int,io.deephaven.vector.ObjectVector) */
    public static <T> T nth( int index, io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.nth( index, values );}
    /** @see io.deephaven.function.ObjectPrimitives#nullToValue(T,T) */
    public static <T> T nullToValue( T value, T defaultValue ) {return ObjectPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.function.BytePrimitives#nullToValue(byte,byte) */
    public static  byte nullToValue( byte value, byte defaultValue ) {return BytePrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.function.CharacterPrimitives#nullToValue(char,char) */
    public static  char nullToValue( char value, char defaultValue ) {return CharacterPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.function.BytePrimitives#nullToValue(byte[],byte) */
    public static  byte[] nullToValue( byte[] values, byte defaultValue ) {return BytePrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.CharacterPrimitives#nullToValue(char[],char) */
    public static  char[] nullToValue( char[] values, char defaultValue ) {return CharacterPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.DoublePrimitives#nullToValue(double[],double) */
    public static  double[] nullToValue( double[] values, double defaultValue ) {return DoublePrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.FloatPrimitives#nullToValue(float[],float) */
    public static  float[] nullToValue( float[] values, float defaultValue ) {return FloatPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.IntegerPrimitives#nullToValue(int[],int) */
    public static  int[] nullToValue( int[] values, int defaultValue ) {return IntegerPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.LongPrimitives#nullToValue(long[],long) */
    public static  long[] nullToValue( long[] values, long defaultValue ) {return LongPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.ShortPrimitives#nullToValue(short[],short) */
    public static  short[] nullToValue( short[] values, short defaultValue ) {return ShortPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.BooleanPrimitives#nullToValue(java.lang.Boolean,boolean) */
    public static  java.lang.Boolean nullToValue( java.lang.Boolean value, boolean defaultValue ) {return BooleanPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.function.DoublePrimitives#nullToValue(double,double) */
    public static  double nullToValue( double value, double defaultValue ) {return DoublePrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.function.FloatPrimitives#nullToValue(float,float) */
    public static  float nullToValue( float value, float defaultValue ) {return FloatPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.function.IntegerPrimitives#nullToValue(int,int) */
    public static  int nullToValue( int value, int defaultValue ) {return IntegerPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.function.BooleanPrimitives#nullToValue(io.deephaven.vector.BooleanVector,boolean) */
    public static  java.lang.Boolean[] nullToValue( io.deephaven.vector.BooleanVector values, boolean defaultValue ) {return BooleanPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.BytePrimitives#nullToValue(io.deephaven.vector.ByteVector,byte) */
    public static  byte[] nullToValue( io.deephaven.vector.ByteVector values, byte defaultValue ) {return BytePrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.CharacterPrimitives#nullToValue(io.deephaven.vector.CharVector,char) */
    public static  char[] nullToValue( io.deephaven.vector.CharVector values, char defaultValue ) {return CharacterPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.DoublePrimitives#nullToValue(io.deephaven.vector.DoubleVector,double) */
    public static  double[] nullToValue( io.deephaven.vector.DoubleVector values, double defaultValue ) {return DoublePrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.FloatPrimitives#nullToValue(io.deephaven.vector.FloatVector,float) */
    public static  float[] nullToValue( io.deephaven.vector.FloatVector values, float defaultValue ) {return FloatPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.IntegerPrimitives#nullToValue(io.deephaven.vector.IntVector,int) */
    public static  int[] nullToValue( io.deephaven.vector.IntVector values, int defaultValue ) {return IntegerPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.LongPrimitives#nullToValue(io.deephaven.vector.LongVector,long) */
    public static  long[] nullToValue( io.deephaven.vector.LongVector values, long defaultValue ) {return LongPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.ShortPrimitives#nullToValue(io.deephaven.vector.ShortVector,short) */
    public static  short[] nullToValue( io.deephaven.vector.ShortVector values, short defaultValue ) {return ShortPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.ObjectPrimitives#nullToValue(io.deephaven.vector.ObjectVector,T) */
    public static <T> T[] nullToValue( io.deephaven.vector.ObjectVector<T> values, T defaultValue ) {return ObjectPrimitives.nullToValue( values, defaultValue );}
    /** @see io.deephaven.function.LongPrimitives#nullToValue(long,long) */
    public static  long nullToValue( long value, long defaultValue ) {return LongPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.function.ShortPrimitives#nullToValue(short,short) */
    public static  short nullToValue( short value, short defaultValue ) {return ShortPrimitives.nullToValue( value, defaultValue );}
    /** @see io.deephaven.function.BooleanPrimitives#or(java.lang.Boolean[]) */
    public static  java.lang.Boolean or( java.lang.Boolean[] values ) {return BooleanPrimitives.or( values );}
    /** @see io.deephaven.function.BooleanPrimitives#or(boolean[]) */
    public static  java.lang.Boolean or( boolean[] values ) {return BooleanPrimitives.or( values );}
    /** @see io.deephaven.function.BooleanPrimitives#or(java.lang.Boolean[],java.lang.Boolean) */
    public static  java.lang.Boolean or( java.lang.Boolean[] values, java.lang.Boolean nullValue ) {return BooleanPrimitives.or( values, nullValue );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#percentile(double[],double) */
    public static  double percentile( double[] values, double percentile ) {return DoubleNumericPrimitives.percentile( values, percentile );}
    /** @see io.deephaven.function.FloatNumericPrimitives#percentile(float[],double) */
    public static  double percentile( float[] values, double percentile ) {return FloatNumericPrimitives.percentile( values, percentile );}
    /** @see io.deephaven.function.ByteNumericPrimitives#percentile(double,byte[]) */
    public static  double percentile( double percentile, byte[] values ) {return ByteNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#percentile(double,int[]) */
    public static  double percentile( double percentile, int[] values ) {return IntegerNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.function.LongNumericPrimitives#percentile(double,long[]) */
    public static  double percentile( double percentile, long[] values ) {return LongNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#percentile(double,short[]) */
    public static  double percentile( double percentile, short[] values ) {return ShortNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#percentile(double,io.deephaven.vector.ByteVector) */
    public static  double percentile( double percentile, io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#percentile(double,io.deephaven.vector.IntVector) */
    public static  double percentile( double percentile, io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.function.LongNumericPrimitives#percentile(double,io.deephaven.vector.LongVector) */
    public static  double percentile( double percentile, io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#percentile(double,io.deephaven.vector.ShortVector) */
    public static  double percentile( double percentile, io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.percentile( percentile, values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#percentile(io.deephaven.vector.DoubleVector,double) */
    public static  double percentile( io.deephaven.vector.DoubleVector values, double percentile ) {return DoubleNumericPrimitives.percentile( values, percentile );}
    /** @see io.deephaven.function.FloatNumericPrimitives#percentile(io.deephaven.vector.FloatVector,double) */
    public static  double percentile( io.deephaven.vector.FloatVector values, double percentile ) {return FloatNumericPrimitives.percentile( values, percentile );}
    /** @see io.deephaven.function.ByteNumericPrimitives#pow(byte,byte) */
    public static  double pow( byte a, byte b ) {return ByteNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#pow(double,double) */
    public static  double pow( double a, double b ) {return DoubleNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.function.FloatNumericPrimitives#pow(float,float) */
    public static  double pow( float a, float b ) {return FloatNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#pow(int,int) */
    public static  double pow( int a, int b ) {return IntegerNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.function.LongNumericPrimitives#pow(long,long) */
    public static  double pow( long a, long b ) {return LongNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.function.ShortNumericPrimitives#pow(short,short) */
    public static  double pow( short a, short b ) {return ShortNumericPrimitives.pow( a, b );}
    /** @see io.deephaven.function.ByteNumericPrimitives#product(byte[]) */
    public static  byte product( byte[] values ) {return ByteNumericPrimitives.product( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#product(double[]) */
    public static  double product( double[] values ) {return DoubleNumericPrimitives.product( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#product(float[]) */
    public static  float product( float[] values ) {return FloatNumericPrimitives.product( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#product(int[]) */
    public static  int product( int[] values ) {return IntegerNumericPrimitives.product( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#product(long[]) */
    public static  long product( long[] values ) {return LongNumericPrimitives.product( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#product(short[]) */
    public static  short product( short[] values ) {return ShortNumericPrimitives.product( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#product(io.deephaven.vector.ByteVector) */
    public static  byte product( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.product( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#product(io.deephaven.vector.DoubleVector) */
    public static  double product( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.product( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#product(io.deephaven.vector.FloatVector) */
    public static  float product( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.product( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#product(io.deephaven.vector.IntVector) */
    public static  int product( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.product( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#product(io.deephaven.vector.LongVector) */
    public static  long product( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.product( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#product(io.deephaven.vector.ShortVector) */
    public static  short product( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.product( values );}
    /** @see io.deephaven.function.SpecialPrimitives#random() */
    public static  double random( ) {return SpecialPrimitives.random( );}
    /** @see io.deephaven.function.SpecialPrimitives#randomBool() */
    public static  boolean randomBool( ) {return SpecialPrimitives.randomBool( );}
    /** @see io.deephaven.function.SpecialPrimitives#randomBool(int) */
    public static  boolean[] randomBool( int size ) {return SpecialPrimitives.randomBool( size );}
    /** @see io.deephaven.function.SpecialPrimitives#randomDouble(double,double) */
    public static  double randomDouble( double min, double max ) {return SpecialPrimitives.randomDouble( min, max );}
    /** @see io.deephaven.function.SpecialPrimitives#randomDouble(double,double,int) */
    public static  double[] randomDouble( double min, double max, int size ) {return SpecialPrimitives.randomDouble( min, max, size );}
    /** @see io.deephaven.function.SpecialPrimitives#randomFloat(float,float) */
    public static  float randomFloat( float min, float max ) {return SpecialPrimitives.randomFloat( min, max );}
    /** @see io.deephaven.function.SpecialPrimitives#randomFloat(float,float,int) */
    public static  float[] randomFloat( float min, float max, int size ) {return SpecialPrimitives.randomFloat( min, max, size );}
    /** @see io.deephaven.function.SpecialPrimitives#randomGaussian(double,double) */
    public static  double randomGaussian( double mean, double std ) {return SpecialPrimitives.randomGaussian( mean, std );}
    /** @see io.deephaven.function.SpecialPrimitives#randomGaussian(double,double,int) */
    public static  double[] randomGaussian( double mean, double std, int size ) {return SpecialPrimitives.randomGaussian( mean, std, size );}
    /** @see io.deephaven.function.SpecialPrimitives#randomInt(int,int) */
    public static  int randomInt( int min, int max ) {return SpecialPrimitives.randomInt( min, max );}
    /** @see io.deephaven.function.SpecialPrimitives#randomInt(int,int,int) */
    public static  int[] randomInt( int min, int max, int size ) {return SpecialPrimitives.randomInt( min, max, size );}
    /** @see io.deephaven.function.SpecialPrimitives#randomLong(long,long) */
    public static  long randomLong( long min, long max ) {return SpecialPrimitives.randomLong( min, max );}
    /** @see io.deephaven.function.SpecialPrimitives#randomLong(long,long,int) */
    public static  long[] randomLong( long min, long max, int size ) {return SpecialPrimitives.randomLong( min, max, size );}
    /** @see io.deephaven.function.ByteNumericPrimitives#rawBinSearchIndex(byte[],byte,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( byte[] values, byte key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ByteNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#rawBinSearchIndex(double[],double,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( double[] values, double key, io.deephaven.function.BinSearch choiceWhenEquals ) {return DoubleNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.FloatNumericPrimitives#rawBinSearchIndex(float[],float,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( float[] values, float key, io.deephaven.function.BinSearch choiceWhenEquals ) {return FloatNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#rawBinSearchIndex(int[],int,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( int[] values, int key, io.deephaven.function.BinSearch choiceWhenEquals ) {return IntegerNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.LongNumericPrimitives#rawBinSearchIndex(long[],long,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( long[] values, long key, io.deephaven.function.BinSearch choiceWhenEquals ) {return LongNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.ShortNumericPrimitives#rawBinSearchIndex(short[],short,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( short[] values, short key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ShortNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.ByteNumericPrimitives#rawBinSearchIndex(io.deephaven.vector.ByteVector,byte,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.vector.ByteVector values, byte key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ByteNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#rawBinSearchIndex(io.deephaven.vector.DoubleVector,double,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.vector.DoubleVector values, double key, io.deephaven.function.BinSearch choiceWhenEquals ) {return DoubleNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.FloatNumericPrimitives#rawBinSearchIndex(io.deephaven.vector.FloatVector,float,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.vector.FloatVector values, float key, io.deephaven.function.BinSearch choiceWhenEquals ) {return FloatNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#rawBinSearchIndex(io.deephaven.vector.IntVector,int,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.vector.IntVector values, int key, io.deephaven.function.BinSearch choiceWhenEquals ) {return IntegerNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.LongNumericPrimitives#rawBinSearchIndex(io.deephaven.vector.LongVector,long,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.vector.LongVector values, long key, io.deephaven.function.BinSearch choiceWhenEquals ) {return LongNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.ShortNumericPrimitives#rawBinSearchIndex(io.deephaven.vector.ShortVector,short,io.deephaven.function.BinSearch) */
    public static  int rawBinSearchIndex( io.deephaven.vector.ShortVector values, short key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ShortNumericPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.ObjectPrimitives#rawBinSearchIndex(io.deephaven.vector.ObjectVector,T,io.deephaven.function.BinSearch) */
    public static <T extends java.lang.Comparable<? super T>> int rawBinSearchIndex( io.deephaven.vector.ObjectVector<T> values, T key, io.deephaven.function.BinSearch choiceWhenEquals ) {return ObjectPrimitives.rawBinSearchIndex( values, key, choiceWhenEquals );}
    /** @see io.deephaven.function.BytePrimitives#repeat(byte,int) */
    public static  byte[] repeat( byte value, int size ) {return BytePrimitives.repeat( value, size );}
    /** @see io.deephaven.function.CharacterPrimitives#repeat(char,int) */
    public static  char[] repeat( char value, int size ) {return CharacterPrimitives.repeat( value, size );}
    /** @see io.deephaven.function.DoublePrimitives#repeat(double,int) */
    public static  double[] repeat( double value, int size ) {return DoublePrimitives.repeat( value, size );}
    /** @see io.deephaven.function.FloatPrimitives#repeat(float,int) */
    public static  float[] repeat( float value, int size ) {return FloatPrimitives.repeat( value, size );}
    /** @see io.deephaven.function.IntegerPrimitives#repeat(int,int) */
    public static  int[] repeat( int value, int size ) {return IntegerPrimitives.repeat( value, size );}
    /** @see io.deephaven.function.LongPrimitives#repeat(long,int) */
    public static  long[] repeat( long value, int size ) {return LongPrimitives.repeat( value, size );}
    /** @see io.deephaven.function.ShortPrimitives#repeat(short,int) */
    public static  short[] repeat( short value, int size ) {return ShortPrimitives.repeat( value, size );}
    /** @see io.deephaven.function.BytePrimitives#reverse(byte[]) */
    public static  byte[] reverse( byte[] values ) {return BytePrimitives.reverse( values );}
    /** @see io.deephaven.function.CharacterPrimitives#reverse(char[]) */
    public static  char[] reverse( char[] values ) {return CharacterPrimitives.reverse( values );}
    /** @see io.deephaven.function.DoublePrimitives#reverse(double[]) */
    public static  double[] reverse( double[] values ) {return DoublePrimitives.reverse( values );}
    /** @see io.deephaven.function.FloatPrimitives#reverse(float[]) */
    public static  float[] reverse( float[] values ) {return FloatPrimitives.reverse( values );}
    /** @see io.deephaven.function.IntegerPrimitives#reverse(int[]) */
    public static  int[] reverse( int[] values ) {return IntegerPrimitives.reverse( values );}
    /** @see io.deephaven.function.LongPrimitives#reverse(long[]) */
    public static  long[] reverse( long[] values ) {return LongPrimitives.reverse( values );}
    /** @see io.deephaven.function.ShortPrimitives#reverse(short[]) */
    public static  short[] reverse( short[] values ) {return ShortPrimitives.reverse( values );}
    /** @see io.deephaven.function.BytePrimitives#reverse(io.deephaven.vector.ByteVector) */
    public static  byte[] reverse( io.deephaven.vector.ByteVector values ) {return BytePrimitives.reverse( values );}
    /** @see io.deephaven.function.CharacterPrimitives#reverse(io.deephaven.vector.CharVector) */
    public static  char[] reverse( io.deephaven.vector.CharVector values ) {return CharacterPrimitives.reverse( values );}
    /** @see io.deephaven.function.DoublePrimitives#reverse(io.deephaven.vector.DoubleVector) */
    public static  double[] reverse( io.deephaven.vector.DoubleVector values ) {return DoublePrimitives.reverse( values );}
    /** @see io.deephaven.function.FloatPrimitives#reverse(io.deephaven.vector.FloatVector) */
    public static  float[] reverse( io.deephaven.vector.FloatVector values ) {return FloatPrimitives.reverse( values );}
    /** @see io.deephaven.function.IntegerPrimitives#reverse(io.deephaven.vector.IntVector) */
    public static  int[] reverse( io.deephaven.vector.IntVector values ) {return IntegerPrimitives.reverse( values );}
    /** @see io.deephaven.function.LongPrimitives#reverse(io.deephaven.vector.LongVector) */
    public static  long[] reverse( io.deephaven.vector.LongVector values ) {return LongPrimitives.reverse( values );}
    /** @see io.deephaven.function.ShortPrimitives#reverse(io.deephaven.vector.ShortVector) */
    public static  short[] reverse( io.deephaven.vector.ShortVector values ) {return ShortPrimitives.reverse( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#rint(byte) */
    public static  double rint( byte value ) {return ByteNumericPrimitives.rint( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#rint(double) */
    public static  double rint( double value ) {return DoubleNumericPrimitives.rint( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#rint(float) */
    public static  double rint( float value ) {return FloatNumericPrimitives.rint( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#rint(int) */
    public static  double rint( int value ) {return IntegerNumericPrimitives.rint( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#rint(long) */
    public static  double rint( long value ) {return LongNumericPrimitives.rint( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#rint(short) */
    public static  double rint( short value ) {return ShortNumericPrimitives.rint( value );}
    /** @see io.deephaven.function.ByteNumericPrimitives#round(byte) */
    public static  long round( byte value ) {return ByteNumericPrimitives.round( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#round(double) */
    public static  long round( double value ) {return DoubleNumericPrimitives.round( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#round(float) */
    public static  long round( float value ) {return FloatNumericPrimitives.round( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#round(int) */
    public static  long round( int value ) {return IntegerNumericPrimitives.round( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#round(long) */
    public static  long round( long value ) {return LongNumericPrimitives.round( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#round(short) */
    public static  long round( short value ) {return ShortNumericPrimitives.round( value );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sequence(byte,byte,byte) */
    public static  byte[] sequence( byte start, byte end, byte step ) {return ByteNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sequence(double,double,double) */
    public static  double[] sequence( double start, double end, double step ) {return DoubleNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sequence(float,float,float) */
    public static  float[] sequence( float start, float end, float step ) {return FloatNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sequence(int,int,int) */
    public static  int[] sequence( int start, int end, int step ) {return IntegerNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.function.LongNumericPrimitives#sequence(long,long,long) */
    public static  long[] sequence( long start, long end, long step ) {return LongNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sequence(short,short,short) */
    public static  short[] sequence( short start, short end, short step ) {return ShortNumericPrimitives.sequence( start, end, step );}
    /** @see io.deephaven.function.ByteNumericPrimitives#signum(byte) */
    public static  byte signum( byte value ) {return ByteNumericPrimitives.signum( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#signum(double) */
    public static  double signum( double value ) {return DoubleNumericPrimitives.signum( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#signum(float) */
    public static  float signum( float value ) {return FloatNumericPrimitives.signum( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#signum(int) */
    public static  int signum( int value ) {return IntegerNumericPrimitives.signum( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#signum(long) */
    public static  long signum( long value ) {return LongNumericPrimitives.signum( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#signum(short) */
    public static  short signum( short value ) {return ShortNumericPrimitives.signum( value );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sin(byte) */
    public static  double sin( byte value ) {return ByteNumericPrimitives.sin( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sin(double) */
    public static  double sin( double value ) {return DoubleNumericPrimitives.sin( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sin(float) */
    public static  double sin( float value ) {return FloatNumericPrimitives.sin( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sin(int) */
    public static  double sin( int value ) {return IntegerNumericPrimitives.sin( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#sin(long) */
    public static  double sin( long value ) {return LongNumericPrimitives.sin( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sin(short) */
    public static  double sin( short value ) {return ShortNumericPrimitives.sin( value );}
    /** @see io.deephaven.function.ObjectPrimitives#sort(NUM[]) */
    public static <NUM extends java.lang.Number> NUM[] sort( NUM[] values ) {return ObjectPrimitives.sort( values );}
    /** @see io.deephaven.function.ObjectPrimitives#sort(T[]) */
    public static <T> T[] sort( T[] values ) {return ObjectPrimitives.sort( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sort(byte[]) */
    public static  byte[] sort( byte[] values ) {return ByteNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sort(double[]) */
    public static  double[] sort( double[] values ) {return DoubleNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sort(float[]) */
    public static  float[] sort( float[] values ) {return FloatNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sort(int[]) */
    public static  int[] sort( int[] values ) {return IntegerNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#sort(long[]) */
    public static  long[] sort( long[] values ) {return LongNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sort(java.lang.Byte[]) */
    public static  byte[] sort( java.lang.Byte[] values ) {return ByteNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sort(java.lang.Double[]) */
    public static  double[] sort( java.lang.Double[] values ) {return DoubleNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sort(java.lang.Float[]) */
    public static  float[] sort( java.lang.Float[] values ) {return FloatNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sort(java.lang.Integer[]) */
    public static  int[] sort( java.lang.Integer[] values ) {return IntegerNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#sort(java.lang.Long[]) */
    public static  long[] sort( java.lang.Long[] values ) {return LongNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sort(java.lang.Short[]) */
    public static  short[] sort( java.lang.Short[] values ) {return ShortNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sort(short[]) */
    public static  short[] sort( short[] values ) {return ShortNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sort(io.deephaven.vector.ByteVector) */
    public static  io.deephaven.vector.ByteVector sort( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sort(io.deephaven.vector.DoubleVector) */
    public static  io.deephaven.vector.DoubleVector sort( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sort(io.deephaven.vector.FloatVector) */
    public static  io.deephaven.vector.FloatVector sort( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sort(io.deephaven.vector.IntVector) */
    public static  io.deephaven.vector.IntVector sort( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#sort(io.deephaven.vector.LongVector) */
    public static  io.deephaven.vector.LongVector sort( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sort(io.deephaven.vector.ShortVector) */
    public static  io.deephaven.vector.ShortVector sort( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.sort( values );}
    /** @see io.deephaven.function.ObjectPrimitives#sort(io.deephaven.vector.ObjectVector) */
    public static <T extends java.lang.Comparable<? super T>> io.deephaven.vector.ObjectVector<T> sort( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.sort( values );}
    /** @see io.deephaven.function.ObjectPrimitives#sortDescending(NUM[]) */
    public static <NUM extends java.lang.Number> NUM[] sortDescending( NUM[] values ) {return ObjectPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.ObjectPrimitives#sortDescending(T[]) */
    public static <T> T[] sortDescending( T[] values ) {return ObjectPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sortDescending(byte[]) */
    public static  byte[] sortDescending( byte[] values ) {return ByteNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sortDescending(double[]) */
    public static  double[] sortDescending( double[] values ) {return DoubleNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sortDescending(float[]) */
    public static  float[] sortDescending( float[] values ) {return FloatNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sortDescending(int[]) */
    public static  int[] sortDescending( int[] values ) {return IntegerNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#sortDescending(long[]) */
    public static  long[] sortDescending( long[] values ) {return LongNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sortDescending(java.lang.Byte[]) */
    public static  byte[] sortDescending( java.lang.Byte[] values ) {return ByteNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sortDescending(java.lang.Double[]) */
    public static  double[] sortDescending( java.lang.Double[] values ) {return DoubleNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sortDescending(java.lang.Float[]) */
    public static  float[] sortDescending( java.lang.Float[] values ) {return FloatNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sortDescending(java.lang.Integer[]) */
    public static  int[] sortDescending( java.lang.Integer[] values ) {return IntegerNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#sortDescending(java.lang.Long[]) */
    public static  long[] sortDescending( java.lang.Long[] values ) {return LongNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sortDescending(java.lang.Short[]) */
    public static  short[] sortDescending( java.lang.Short[] values ) {return ShortNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sortDescending(short[]) */
    public static  short[] sortDescending( short[] values ) {return ShortNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sortDescending(io.deephaven.vector.ByteVector) */
    public static  io.deephaven.vector.ByteVector sortDescending( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sortDescending(io.deephaven.vector.DoubleVector) */
    public static  io.deephaven.vector.DoubleVector sortDescending( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sortDescending(io.deephaven.vector.FloatVector) */
    public static  io.deephaven.vector.FloatVector sortDescending( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sortDescending(io.deephaven.vector.IntVector) */
    public static  io.deephaven.vector.IntVector sortDescending( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#sortDescending(io.deephaven.vector.LongVector) */
    public static  io.deephaven.vector.LongVector sortDescending( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sortDescending(io.deephaven.vector.ShortVector) */
    public static  io.deephaven.vector.ShortVector sortDescending( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.ObjectPrimitives#sortDescending(io.deephaven.vector.ObjectVector) */
    public static <T extends java.lang.Comparable<? super T>> io.deephaven.vector.ObjectVector<T> sortDescending( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.sortDescending( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sqrt(byte) */
    public static  double sqrt( byte value ) {return ByteNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sqrt(double) */
    public static  double sqrt( double value ) {return DoubleNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sqrt(float) */
    public static  double sqrt( float value ) {return FloatNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sqrt(int) */
    public static  double sqrt( int value ) {return IntegerNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#sqrt(long) */
    public static  double sqrt( long value ) {return LongNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sqrt(short) */
    public static  double sqrt( short value ) {return ShortNumericPrimitives.sqrt( value );}
    /** @see io.deephaven.function.ByteNumericPrimitives#std(byte[]) */
    public static  double std( byte[] values ) {return ByteNumericPrimitives.std( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#std(double[]) */
    public static  double std( double[] values ) {return DoubleNumericPrimitives.std( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#std(float[]) */
    public static  double std( float[] values ) {return FloatNumericPrimitives.std( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#std(int[]) */
    public static  double std( int[] values ) {return IntegerNumericPrimitives.std( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#std(long[]) */
    public static  double std( long[] values ) {return LongNumericPrimitives.std( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#std(java.lang.Byte[]) */
    public static  double std( java.lang.Byte[] values ) {return ByteNumericPrimitives.std( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#std(java.lang.Double[]) */
    public static  double std( java.lang.Double[] values ) {return DoubleNumericPrimitives.std( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#std(java.lang.Float[]) */
    public static  double std( java.lang.Float[] values ) {return FloatNumericPrimitives.std( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#std(java.lang.Integer[]) */
    public static  double std( java.lang.Integer[] values ) {return IntegerNumericPrimitives.std( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#std(java.lang.Long[]) */
    public static  double std( java.lang.Long[] values ) {return LongNumericPrimitives.std( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#std(java.lang.Short[]) */
    public static  double std( java.lang.Short[] values ) {return ShortNumericPrimitives.std( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#std(short[]) */
    public static  double std( short[] values ) {return ShortNumericPrimitives.std( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#std(io.deephaven.vector.ByteVector) */
    public static  double std( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.std( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#std(io.deephaven.vector.DoubleVector) */
    public static  double std( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.std( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#std(io.deephaven.vector.FloatVector) */
    public static  double std( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.std( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#std(io.deephaven.vector.IntVector) */
    public static  double std( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.std( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#std(io.deephaven.vector.LongVector) */
    public static  double std( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.std( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#std(io.deephaven.vector.ShortVector) */
    public static  double std( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.std( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#ste(byte[]) */
    public static  double ste( byte[] values ) {return ByteNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#ste(double[]) */
    public static  double ste( double[] values ) {return DoubleNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#ste(float[]) */
    public static  double ste( float[] values ) {return FloatNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#ste(int[]) */
    public static  double ste( int[] values ) {return IntegerNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#ste(long[]) */
    public static  double ste( long[] values ) {return LongNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#ste(java.lang.Byte[]) */
    public static  double ste( java.lang.Byte[] values ) {return ByteNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#ste(java.lang.Double[]) */
    public static  double ste( java.lang.Double[] values ) {return DoubleNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#ste(java.lang.Float[]) */
    public static  double ste( java.lang.Float[] values ) {return FloatNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#ste(java.lang.Integer[]) */
    public static  double ste( java.lang.Integer[] values ) {return IntegerNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#ste(java.lang.Long[]) */
    public static  double ste( java.lang.Long[] values ) {return LongNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#ste(java.lang.Short[]) */
    public static  double ste( java.lang.Short[] values ) {return ShortNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#ste(short[]) */
    public static  double ste( short[] values ) {return ShortNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#ste(io.deephaven.vector.ByteVector) */
    public static  double ste( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#ste(io.deephaven.vector.DoubleVector) */
    public static  double ste( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#ste(io.deephaven.vector.FloatVector) */
    public static  double ste( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#ste(io.deephaven.vector.IntVector) */
    public static  double ste( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#ste(io.deephaven.vector.LongVector) */
    public static  double ste( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#ste(io.deephaven.vector.ShortVector) */
    public static  double ste( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.ste( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sum(byte[]) */
    public static  byte sum( byte[] values ) {return ByteNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sum(double[]) */
    public static  double sum( double[] values ) {return DoubleNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sum(float[]) */
    public static  float sum( float[] values ) {return FloatNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sum(int[]) */
    public static  int sum( int[] values ) {return IntegerNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#sum(long[]) */
    public static  long sum( long[] values ) {return LongNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.BooleanPrimitives#sum(java.lang.Boolean[]) */
    public static  java.lang.Boolean sum( java.lang.Boolean[] values ) {return BooleanPrimitives.sum( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sum(short[]) */
    public static  short sum( short[] values ) {return ShortNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.BooleanPrimitives#sum(boolean[]) */
    public static  java.lang.Boolean sum( boolean[] values ) {return BooleanPrimitives.sum( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sum(byte[][]) */
    public static  byte[] sum( byte[][] values ) {return ByteNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sum(double[][]) */
    public static  double[] sum( double[][] values ) {return DoubleNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sum(float[][]) */
    public static  float[] sum( float[][] values ) {return FloatNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sum(int[][]) */
    public static  int[] sum( int[][] values ) {return IntegerNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#sum(long[][]) */
    public static  long[] sum( long[][] values ) {return LongNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sum(short[][]) */
    public static  short[] sum( short[][] values ) {return ShortNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#sum(io.deephaven.vector.ByteVector) */
    public static  byte sum( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#sum(io.deephaven.vector.DoubleVector) */
    public static  double sum( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#sum(io.deephaven.vector.FloatVector) */
    public static  float sum( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#sum(io.deephaven.vector.IntVector) */
    public static  int sum( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#sum(io.deephaven.vector.LongVector) */
    public static  long sum( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#sum(io.deephaven.vector.ShortVector) */
    public static  short sum( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.sum( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#tan(byte) */
    public static  double tan( byte value ) {return ByteNumericPrimitives.tan( value );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#tan(double) */
    public static  double tan( double value ) {return DoubleNumericPrimitives.tan( value );}
    /** @see io.deephaven.function.FloatNumericPrimitives#tan(float) */
    public static  double tan( float value ) {return FloatNumericPrimitives.tan( value );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#tan(int) */
    public static  double tan( int value ) {return IntegerNumericPrimitives.tan( value );}
    /** @see io.deephaven.function.LongNumericPrimitives#tan(long) */
    public static  double tan( long value ) {return LongNumericPrimitives.tan( value );}
    /** @see io.deephaven.function.ShortNumericPrimitives#tan(short) */
    public static  double tan( short value ) {return ShortNumericPrimitives.tan( value );}
    /** @see io.deephaven.function.ByteNumericPrimitives#tstat(byte[]) */
    public static  double tstat( byte[] values ) {return ByteNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#tstat(double[]) */
    public static  double tstat( double[] values ) {return DoubleNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#tstat(float[]) */
    public static  double tstat( float[] values ) {return FloatNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#tstat(int[]) */
    public static  double tstat( int[] values ) {return IntegerNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#tstat(long[]) */
    public static  double tstat( long[] values ) {return LongNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#tstat(java.lang.Byte[]) */
    public static  double tstat( java.lang.Byte[] values ) {return ByteNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#tstat(java.lang.Double[]) */
    public static  double tstat( java.lang.Double[] values ) {return DoubleNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#tstat(java.lang.Float[]) */
    public static  double tstat( java.lang.Float[] values ) {return FloatNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#tstat(java.lang.Integer[]) */
    public static  double tstat( java.lang.Integer[] values ) {return IntegerNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#tstat(java.lang.Long[]) */
    public static  double tstat( java.lang.Long[] values ) {return LongNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#tstat(java.lang.Short[]) */
    public static  double tstat( java.lang.Short[] values ) {return ShortNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#tstat(short[]) */
    public static  double tstat( short[] values ) {return ShortNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#tstat(io.deephaven.vector.ByteVector) */
    public static  double tstat( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#tstat(io.deephaven.vector.DoubleVector) */
    public static  double tstat( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#tstat(io.deephaven.vector.FloatVector) */
    public static  double tstat( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#tstat(io.deephaven.vector.IntVector) */
    public static  double tstat( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#tstat(io.deephaven.vector.LongVector) */
    public static  double tstat( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#tstat(io.deephaven.vector.ShortVector) */
    public static  double tstat( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.tstat( values );}
    /** @see io.deephaven.function.BytePrimitives#unbox(java.lang.Byte[]) */
    public static  byte[] unbox( java.lang.Byte[] values ) {return BytePrimitives.unbox( values );}
    /** @see io.deephaven.function.CharacterPrimitives#unbox(java.lang.Character[]) */
    public static  char[] unbox( java.lang.Character[] values ) {return CharacterPrimitives.unbox( values );}
    /** @see io.deephaven.function.DoublePrimitives#unbox(java.lang.Double[]) */
    public static  double[] unbox( java.lang.Double[] values ) {return DoublePrimitives.unbox( values );}
    /** @see io.deephaven.function.FloatPrimitives#unbox(java.lang.Float[]) */
    public static  float[] unbox( java.lang.Float[] values ) {return FloatPrimitives.unbox( values );}
    /** @see io.deephaven.function.IntegerPrimitives#unbox(java.lang.Integer[]) */
    public static  int[] unbox( java.lang.Integer[] values ) {return IntegerPrimitives.unbox( values );}
    /** @see io.deephaven.function.LongPrimitives#unbox(java.lang.Long[]) */
    public static  long[] unbox( java.lang.Long[] values ) {return LongPrimitives.unbox( values );}
    /** @see io.deephaven.function.ShortPrimitives#unbox(java.lang.Short[]) */
    public static  short[] unbox( java.lang.Short[] values ) {return ShortPrimitives.unbox( values );}
    /** @see io.deephaven.function.BytePrimitives#uniqueValue(io.deephaven.vector.ByteVector,boolean) */
    public static  byte uniqueValue( io.deephaven.vector.ByteVector arr, boolean countNull ) {return BytePrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.function.CharacterPrimitives#uniqueValue(io.deephaven.vector.CharVector,boolean) */
    public static  char uniqueValue( io.deephaven.vector.CharVector arr, boolean countNull ) {return CharacterPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.function.DoublePrimitives#uniqueValue(io.deephaven.vector.DoubleVector,boolean) */
    public static  double uniqueValue( io.deephaven.vector.DoubleVector arr, boolean countNull ) {return DoublePrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.function.FloatPrimitives#uniqueValue(io.deephaven.vector.FloatVector,boolean) */
    public static  float uniqueValue( io.deephaven.vector.FloatVector arr, boolean countNull ) {return FloatPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.function.IntegerPrimitives#uniqueValue(io.deephaven.vector.IntVector,boolean) */
    public static  int uniqueValue( io.deephaven.vector.IntVector arr, boolean countNull ) {return IntegerPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.function.LongPrimitives#uniqueValue(io.deephaven.vector.LongVector,boolean) */
    public static  long uniqueValue( io.deephaven.vector.LongVector arr, boolean countNull ) {return LongPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.function.ShortPrimitives#uniqueValue(io.deephaven.vector.ShortVector,boolean) */
    public static  short uniqueValue( io.deephaven.vector.ShortVector arr, boolean countNull ) {return ShortPrimitives.uniqueValue( arr, countNull );}
    /** @see io.deephaven.function.ByteNumericPrimitives#upperBin(byte,byte) */
    public static  byte upperBin( byte value, byte interval ) {return ByteNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#upperBin(double,double) */
    public static  double upperBin( double value, double interval ) {return DoubleNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.function.FloatNumericPrimitives#upperBin(float,float) */
    public static  float upperBin( float value, float interval ) {return FloatNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#upperBin(int,int) */
    public static  int upperBin( int value, int interval ) {return IntegerNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.function.LongNumericPrimitives#upperBin(long,long) */
    public static  long upperBin( long value, long interval ) {return LongNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.function.ShortNumericPrimitives#upperBin(short,short) */
    public static  short upperBin( short value, short interval ) {return ShortNumericPrimitives.upperBin( value, interval );}
    /** @see io.deephaven.function.ByteNumericPrimitives#upperBin(byte,byte,byte) */
    public static  byte upperBin( byte value, byte interval, byte offset ) {return ByteNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#upperBin(double,double,double) */
    public static  double upperBin( double value, double interval, double offset ) {return DoubleNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.function.FloatNumericPrimitives#upperBin(float,float,float) */
    public static  float upperBin( float value, float interval, float offset ) {return FloatNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#upperBin(int,int,int) */
    public static  int upperBin( int value, int interval, int offset ) {return IntegerNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.function.LongNumericPrimitives#upperBin(long,long,long) */
    public static  long upperBin( long value, long interval, long offset ) {return LongNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.function.ShortNumericPrimitives#upperBin(short,short,short) */
    public static  short upperBin( short value, short interval, short offset ) {return ShortNumericPrimitives.upperBin( value, interval, offset );}
    /** @see io.deephaven.function.ByteNumericPrimitives#var(byte[]) */
    public static  double var( byte[] values ) {return ByteNumericPrimitives.var( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#var(double[]) */
    public static  double var( double[] values ) {return DoubleNumericPrimitives.var( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#var(float[]) */
    public static  double var( float[] values ) {return FloatNumericPrimitives.var( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#var(int[]) */
    public static  double var( int[] values ) {return IntegerNumericPrimitives.var( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#var(long[]) */
    public static  double var( long[] values ) {return LongNumericPrimitives.var( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#var(java.lang.Byte[]) */
    public static  double var( java.lang.Byte[] values ) {return ByteNumericPrimitives.var( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#var(java.lang.Double[]) */
    public static  double var( java.lang.Double[] values ) {return DoubleNumericPrimitives.var( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#var(java.lang.Float[]) */
    public static  double var( java.lang.Float[] values ) {return FloatNumericPrimitives.var( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#var(java.lang.Integer[]) */
    public static  double var( java.lang.Integer[] values ) {return IntegerNumericPrimitives.var( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#var(java.lang.Long[]) */
    public static  double var( java.lang.Long[] values ) {return LongNumericPrimitives.var( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#var(java.lang.Short[]) */
    public static  double var( java.lang.Short[] values ) {return ShortNumericPrimitives.var( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#var(short[]) */
    public static  double var( short[] values ) {return ShortNumericPrimitives.var( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#var(io.deephaven.vector.ByteVector) */
    public static  double var( io.deephaven.vector.ByteVector values ) {return ByteNumericPrimitives.var( values );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#var(io.deephaven.vector.DoubleVector) */
    public static  double var( io.deephaven.vector.DoubleVector values ) {return DoubleNumericPrimitives.var( values );}
    /** @see io.deephaven.function.FloatNumericPrimitives#var(io.deephaven.vector.FloatVector) */
    public static  double var( io.deephaven.vector.FloatVector values ) {return FloatNumericPrimitives.var( values );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#var(io.deephaven.vector.IntVector) */
    public static  double var( io.deephaven.vector.IntVector values ) {return IntegerNumericPrimitives.var( values );}
    /** @see io.deephaven.function.LongNumericPrimitives#var(io.deephaven.vector.LongVector) */
    public static  double var( io.deephaven.vector.LongVector values ) {return LongNumericPrimitives.var( values );}
    /** @see io.deephaven.function.ShortNumericPrimitives#var(io.deephaven.vector.ShortVector) */
    public static  double var( io.deephaven.vector.ShortVector values ) {return ShortNumericPrimitives.var( values );}
    /** @see io.deephaven.function.BooleanPrimitives#vec(io.deephaven.vector.BooleanVector) */
    public static  java.lang.Boolean[] vec( io.deephaven.vector.BooleanVector values ) {return BooleanPrimitives.vec( values );}
    /** @see io.deephaven.function.BytePrimitives#vec(io.deephaven.vector.ByteVector) */
    public static  byte[] vec( io.deephaven.vector.ByteVector values ) {return BytePrimitives.vec( values );}
    /** @see io.deephaven.function.CharacterPrimitives#vec(io.deephaven.vector.CharVector) */
    public static  char[] vec( io.deephaven.vector.CharVector values ) {return CharacterPrimitives.vec( values );}
    /** @see io.deephaven.function.DoublePrimitives#vec(io.deephaven.vector.DoubleVector) */
    public static  double[] vec( io.deephaven.vector.DoubleVector values ) {return DoublePrimitives.vec( values );}
    /** @see io.deephaven.function.FloatPrimitives#vec(io.deephaven.vector.FloatVector) */
    public static  float[] vec( io.deephaven.vector.FloatVector values ) {return FloatPrimitives.vec( values );}
    /** @see io.deephaven.function.IntegerPrimitives#vec(io.deephaven.vector.IntVector) */
    public static  int[] vec( io.deephaven.vector.IntVector values ) {return IntegerPrimitives.vec( values );}
    /** @see io.deephaven.function.LongPrimitives#vec(io.deephaven.vector.LongVector) */
    public static  long[] vec( io.deephaven.vector.LongVector values ) {return LongPrimitives.vec( values );}
    /** @see io.deephaven.function.ShortPrimitives#vec(io.deephaven.vector.ShortVector) */
    public static  short[] vec( io.deephaven.vector.ShortVector values ) {return ShortPrimitives.vec( values );}
    /** @see io.deephaven.function.ObjectPrimitives#vec(io.deephaven.vector.ObjectVector) */
    public static <T> T[] vec( io.deephaven.vector.ObjectVector<T> values ) {return ObjectPrimitives.vec( values );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],byte[]) */
    public static  double wavg( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],double[]) */
    public static  double wavg( byte[] values, double[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],float[]) */
    public static  double wavg( byte[] values, float[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],int[]) */
    public static  double wavg( byte[] values, int[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],long[]) */
    public static  double wavg( byte[] values, long[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],io.deephaven.vector.ByteVector) */
    public static  double wavg( byte[] values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],io.deephaven.vector.DoubleVector) */
    public static  double wavg( byte[] values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],io.deephaven.vector.FloatVector) */
    public static  double wavg( byte[] values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],io.deephaven.vector.IntVector) */
    public static  double wavg( byte[] values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(byte[],io.deephaven.vector.LongVector) */
    public static  double wavg( byte[] values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],double[]) */
    public static  double wavg( double[] values, double[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],float[]) */
    public static  double wavg( double[] values, float[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],int[]) */
    public static  double wavg( double[] values, int[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],long[]) */
    public static  double wavg( double[] values, long[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],short[]) */
    public static  double wavg( double[] values, short[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],io.deephaven.vector.DoubleVector) */
    public static  double wavg( double[] values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],io.deephaven.vector.FloatVector) */
    public static  double wavg( double[] values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],io.deephaven.vector.IntVector) */
    public static  double wavg( double[] values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],io.deephaven.vector.LongVector) */
    public static  double wavg( double[] values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(double[],io.deephaven.vector.ShortVector) */
    public static  double wavg( double[] values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],double[]) */
    public static  double wavg( float[] values, double[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],float[]) */
    public static  double wavg( float[] values, float[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],int[]) */
    public static  double wavg( float[] values, int[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],long[]) */
    public static  double wavg( float[] values, long[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],short[]) */
    public static  double wavg( float[] values, short[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],io.deephaven.vector.DoubleVector) */
    public static  double wavg( float[] values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],io.deephaven.vector.FloatVector) */
    public static  double wavg( float[] values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],io.deephaven.vector.IntVector) */
    public static  double wavg( float[] values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],io.deephaven.vector.LongVector) */
    public static  double wavg( float[] values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(float[],io.deephaven.vector.ShortVector) */
    public static  double wavg( float[] values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],double[]) */
    public static  double wavg( int[] values, double[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],float[]) */
    public static  double wavg( int[] values, float[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],int[]) */
    public static  double wavg( int[] values, int[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],long[]) */
    public static  double wavg( int[] values, long[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],short[]) */
    public static  double wavg( int[] values, short[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],io.deephaven.vector.DoubleVector) */
    public static  double wavg( int[] values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],io.deephaven.vector.FloatVector) */
    public static  double wavg( int[] values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],io.deephaven.vector.IntVector) */
    public static  double wavg( int[] values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],io.deephaven.vector.LongVector) */
    public static  double wavg( int[] values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(int[],io.deephaven.vector.ShortVector) */
    public static  double wavg( int[] values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],double[]) */
    public static  double wavg( long[] values, double[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],float[]) */
    public static  double wavg( long[] values, float[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],int[]) */
    public static  double wavg( long[] values, int[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],long[]) */
    public static  double wavg( long[] values, long[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],short[]) */
    public static  double wavg( long[] values, short[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],io.deephaven.vector.DoubleVector) */
    public static  double wavg( long[] values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],io.deephaven.vector.FloatVector) */
    public static  double wavg( long[] values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],io.deephaven.vector.IntVector) */
    public static  double wavg( long[] values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],io.deephaven.vector.LongVector) */
    public static  double wavg( long[] values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(long[],io.deephaven.vector.ShortVector) */
    public static  double wavg( long[] values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],double[]) */
    public static  double wavg( short[] values, double[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],float[]) */
    public static  double wavg( short[] values, float[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],int[]) */
    public static  double wavg( short[] values, int[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],long[]) */
    public static  double wavg( short[] values, long[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],short[]) */
    public static  double wavg( short[] values, short[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],io.deephaven.vector.DoubleVector) */
    public static  double wavg( short[] values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],io.deephaven.vector.FloatVector) */
    public static  double wavg( short[] values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],io.deephaven.vector.IntVector) */
    public static  double wavg( short[] values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],io.deephaven.vector.LongVector) */
    public static  double wavg( short[] values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(short[],io.deephaven.vector.ShortVector) */
    public static  double wavg( short[] values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,byte[]) */
    public static  double wavg( io.deephaven.vector.ByteVector values, byte[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,double[]) */
    public static  double wavg( io.deephaven.vector.ByteVector values, double[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,float[]) */
    public static  double wavg( io.deephaven.vector.ByteVector values, float[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,int[]) */
    public static  double wavg( io.deephaven.vector.ByteVector values, int[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,long[]) */
    public static  double wavg( io.deephaven.vector.ByteVector values, long[] weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double wavg( io.deephaven.vector.ByteVector values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,io.deephaven.vector.DoubleVector) */
    public static  double wavg( io.deephaven.vector.ByteVector values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,io.deephaven.vector.FloatVector) */
    public static  double wavg( io.deephaven.vector.ByteVector values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,io.deephaven.vector.IntVector) */
    public static  double wavg( io.deephaven.vector.ByteVector values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wavg(io.deephaven.vector.ByteVector,io.deephaven.vector.LongVector) */
    public static  double wavg( io.deephaven.vector.ByteVector values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,double[]) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, double[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,float[]) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, float[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,int[]) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, int[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,long[]) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, long[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,short[]) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, short[] weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,io.deephaven.vector.FloatVector) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,io.deephaven.vector.IntVector) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,io.deephaven.vector.LongVector) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wavg(io.deephaven.vector.DoubleVector,io.deephaven.vector.ShortVector) */
    public static  double wavg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,double[]) */
    public static  double wavg( io.deephaven.vector.FloatVector values, double[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,float[]) */
    public static  double wavg( io.deephaven.vector.FloatVector values, float[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,int[]) */
    public static  double wavg( io.deephaven.vector.FloatVector values, int[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,long[]) */
    public static  double wavg( io.deephaven.vector.FloatVector values, long[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,short[]) */
    public static  double wavg( io.deephaven.vector.FloatVector values, short[] weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,io.deephaven.vector.DoubleVector) */
    public static  double wavg( io.deephaven.vector.FloatVector values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double wavg( io.deephaven.vector.FloatVector values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,io.deephaven.vector.IntVector) */
    public static  double wavg( io.deephaven.vector.FloatVector values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,io.deephaven.vector.LongVector) */
    public static  double wavg( io.deephaven.vector.FloatVector values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wavg(io.deephaven.vector.FloatVector,io.deephaven.vector.ShortVector) */
    public static  double wavg( io.deephaven.vector.FloatVector values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,double[]) */
    public static  double wavg( io.deephaven.vector.IntVector values, double[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,float[]) */
    public static  double wavg( io.deephaven.vector.IntVector values, float[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,int[]) */
    public static  double wavg( io.deephaven.vector.IntVector values, int[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,long[]) */
    public static  double wavg( io.deephaven.vector.IntVector values, long[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,short[]) */
    public static  double wavg( io.deephaven.vector.IntVector values, short[] weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,io.deephaven.vector.DoubleVector) */
    public static  double wavg( io.deephaven.vector.IntVector values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,io.deephaven.vector.FloatVector) */
    public static  double wavg( io.deephaven.vector.IntVector values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double wavg( io.deephaven.vector.IntVector values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,io.deephaven.vector.LongVector) */
    public static  double wavg( io.deephaven.vector.IntVector values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wavg(io.deephaven.vector.IntVector,io.deephaven.vector.ShortVector) */
    public static  double wavg( io.deephaven.vector.IntVector values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,double[]) */
    public static  double wavg( io.deephaven.vector.LongVector values, double[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,float[]) */
    public static  double wavg( io.deephaven.vector.LongVector values, float[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,int[]) */
    public static  double wavg( io.deephaven.vector.LongVector values, int[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,long[]) */
    public static  double wavg( io.deephaven.vector.LongVector values, long[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,short[]) */
    public static  double wavg( io.deephaven.vector.LongVector values, short[] weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,io.deephaven.vector.DoubleVector) */
    public static  double wavg( io.deephaven.vector.LongVector values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,io.deephaven.vector.FloatVector) */
    public static  double wavg( io.deephaven.vector.LongVector values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,io.deephaven.vector.IntVector) */
    public static  double wavg( io.deephaven.vector.LongVector values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double wavg( io.deephaven.vector.LongVector values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wavg(io.deephaven.vector.LongVector,io.deephaven.vector.ShortVector) */
    public static  double wavg( io.deephaven.vector.LongVector values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,double[]) */
    public static  double wavg( io.deephaven.vector.ShortVector values, double[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,float[]) */
    public static  double wavg( io.deephaven.vector.ShortVector values, float[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,int[]) */
    public static  double wavg( io.deephaven.vector.ShortVector values, int[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,long[]) */
    public static  double wavg( io.deephaven.vector.ShortVector values, long[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,short[]) */
    public static  double wavg( io.deephaven.vector.ShortVector values, short[] weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,io.deephaven.vector.DoubleVector) */
    public static  double wavg( io.deephaven.vector.ShortVector values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,io.deephaven.vector.FloatVector) */
    public static  double wavg( io.deephaven.vector.ShortVector values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,io.deephaven.vector.IntVector) */
    public static  double wavg( io.deephaven.vector.ShortVector values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,io.deephaven.vector.LongVector) */
    public static  double wavg( io.deephaven.vector.ShortVector values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wavg(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double wavg( io.deephaven.vector.ShortVector values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wavg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],byte[]) */
    public static  double weightedAvg( byte[] values, byte[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],double[]) */
    public static  double weightedAvg( byte[] values, double[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],float[]) */
    public static  double weightedAvg( byte[] values, float[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],int[]) */
    public static  double weightedAvg( byte[] values, int[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],long[]) */
    public static  double weightedAvg( byte[] values, long[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.vector.ByteVector) */
    public static  double weightedAvg( byte[] values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( byte[] values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( byte[] values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.vector.IntVector) */
    public static  double weightedAvg( byte[] values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(byte[],io.deephaven.vector.LongVector) */
    public static  double weightedAvg( byte[] values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],double[]) */
    public static  double weightedAvg( double[] values, double[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],float[]) */
    public static  double weightedAvg( double[] values, float[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],int[]) */
    public static  double weightedAvg( double[] values, int[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],long[]) */
    public static  double weightedAvg( double[] values, long[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],short[]) */
    public static  double weightedAvg( double[] values, short[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( double[] values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( double[] values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.vector.IntVector) */
    public static  double weightedAvg( double[] values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.vector.LongVector) */
    public static  double weightedAvg( double[] values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(double[],io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( double[] values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],double[]) */
    public static  double weightedAvg( float[] values, double[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],float[]) */
    public static  double weightedAvg( float[] values, float[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],int[]) */
    public static  double weightedAvg( float[] values, int[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],long[]) */
    public static  double weightedAvg( float[] values, long[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],short[]) */
    public static  double weightedAvg( float[] values, short[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( float[] values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( float[] values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.vector.IntVector) */
    public static  double weightedAvg( float[] values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.vector.LongVector) */
    public static  double weightedAvg( float[] values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(float[],io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( float[] values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],double[]) */
    public static  double weightedAvg( int[] values, double[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],float[]) */
    public static  double weightedAvg( int[] values, float[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],int[]) */
    public static  double weightedAvg( int[] values, int[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],long[]) */
    public static  double weightedAvg( int[] values, long[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],short[]) */
    public static  double weightedAvg( int[] values, short[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( int[] values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( int[] values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.vector.IntVector) */
    public static  double weightedAvg( int[] values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.vector.LongVector) */
    public static  double weightedAvg( int[] values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(int[],io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( int[] values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],double[]) */
    public static  double weightedAvg( long[] values, double[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],float[]) */
    public static  double weightedAvg( long[] values, float[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],int[]) */
    public static  double weightedAvg( long[] values, int[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],long[]) */
    public static  double weightedAvg( long[] values, long[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],short[]) */
    public static  double weightedAvg( long[] values, short[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( long[] values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( long[] values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],io.deephaven.vector.IntVector) */
    public static  double weightedAvg( long[] values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],io.deephaven.vector.LongVector) */
    public static  double weightedAvg( long[] values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(long[],io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( long[] values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],double[]) */
    public static  double weightedAvg( short[] values, double[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],float[]) */
    public static  double weightedAvg( short[] values, float[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],int[]) */
    public static  double weightedAvg( short[] values, int[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],long[]) */
    public static  double weightedAvg( short[] values, long[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],short[]) */
    public static  double weightedAvg( short[] values, short[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( short[] values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( short[] values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.vector.IntVector) */
    public static  double weightedAvg( short[] values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.vector.LongVector) */
    public static  double weightedAvg( short[] values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(short[],io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( short[] values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,byte[]) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, byte[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,double[]) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, double[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,float[]) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, float[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,int[]) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, int[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,long[]) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, long[] weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,io.deephaven.vector.IntVector) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedAvg(io.deephaven.vector.ByteVector,io.deephaven.vector.LongVector) */
    public static  double weightedAvg( io.deephaven.vector.ByteVector values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,double[]) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, double[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,float[]) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, float[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,int[]) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, int[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,long[]) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, long[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,short[]) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, short[] weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,io.deephaven.vector.IntVector) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,io.deephaven.vector.LongVector) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedAvg(io.deephaven.vector.DoubleVector,io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( io.deephaven.vector.DoubleVector values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,double[]) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, double[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,float[]) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, float[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,int[]) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, int[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,long[]) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, long[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,short[]) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, short[] weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,io.deephaven.vector.IntVector) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,io.deephaven.vector.LongVector) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedAvg(io.deephaven.vector.FloatVector,io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( io.deephaven.vector.FloatVector values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,double[]) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, double[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,float[]) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, float[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,int[]) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, int[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,long[]) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, long[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,short[]) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, short[] weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,io.deephaven.vector.LongVector) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedAvg(io.deephaven.vector.IntVector,io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( io.deephaven.vector.IntVector values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,double[]) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, double[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,float[]) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, float[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,int[]) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, int[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,long[]) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, long[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,short[]) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, short[] weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,io.deephaven.vector.IntVector) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedAvg(io.deephaven.vector.LongVector,io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( io.deephaven.vector.LongVector values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,double[]) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, double[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,float[]) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, float[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,int[]) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, int[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,long[]) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, long[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,short[]) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, short[] weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,io.deephaven.vector.FloatVector) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,io.deephaven.vector.IntVector) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,io.deephaven.vector.LongVector) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedAvg(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double weightedAvg( io.deephaven.vector.ShortVector values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.weightedAvg( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],byte[]) */
    public static  double weightedSum( byte[] values, byte[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],double[]) */
    public static  double weightedSum( byte[] values, double[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],float[]) */
    public static  double weightedSum( byte[] values, float[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],int[]) */
    public static  double weightedSum( byte[] values, int[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],long[]) */
    public static  double weightedSum( byte[] values, long[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.vector.ByteVector) */
    public static  double weightedSum( byte[] values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( byte[] values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.vector.FloatVector) */
    public static  double weightedSum( byte[] values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.vector.IntVector) */
    public static  double weightedSum( byte[] values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(byte[],io.deephaven.vector.LongVector) */
    public static  double weightedSum( byte[] values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],double[]) */
    public static  double weightedSum( double[] values, double[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],float[]) */
    public static  double weightedSum( double[] values, float[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],int[]) */
    public static  double weightedSum( double[] values, int[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],long[]) */
    public static  double weightedSum( double[] values, long[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],short[]) */
    public static  double weightedSum( double[] values, short[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( double[] values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.vector.FloatVector) */
    public static  double weightedSum( double[] values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.vector.IntVector) */
    public static  double weightedSum( double[] values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.vector.LongVector) */
    public static  double weightedSum( double[] values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(double[],io.deephaven.vector.ShortVector) */
    public static  double weightedSum( double[] values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],double[]) */
    public static  double weightedSum( float[] values, double[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],float[]) */
    public static  double weightedSum( float[] values, float[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],int[]) */
    public static  double weightedSum( float[] values, int[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],long[]) */
    public static  double weightedSum( float[] values, long[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],short[]) */
    public static  double weightedSum( float[] values, short[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( float[] values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],io.deephaven.vector.FloatVector) */
    public static  double weightedSum( float[] values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],io.deephaven.vector.IntVector) */
    public static  double weightedSum( float[] values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],io.deephaven.vector.LongVector) */
    public static  double weightedSum( float[] values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(float[],io.deephaven.vector.ShortVector) */
    public static  double weightedSum( float[] values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],double[]) */
    public static  double weightedSum( int[] values, double[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],float[]) */
    public static  double weightedSum( int[] values, float[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],int[]) */
    public static  double weightedSum( int[] values, int[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],long[]) */
    public static  double weightedSum( int[] values, long[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],short[]) */
    public static  double weightedSum( int[] values, short[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( int[] values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.vector.FloatVector) */
    public static  double weightedSum( int[] values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.vector.IntVector) */
    public static  double weightedSum( int[] values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.vector.LongVector) */
    public static  double weightedSum( int[] values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(int[],io.deephaven.vector.ShortVector) */
    public static  double weightedSum( int[] values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],double[]) */
    public static  double weightedSum( long[] values, double[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],float[]) */
    public static  double weightedSum( long[] values, float[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],int[]) */
    public static  double weightedSum( long[] values, int[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],long[]) */
    public static  double weightedSum( long[] values, long[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],short[]) */
    public static  double weightedSum( long[] values, short[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( long[] values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],io.deephaven.vector.FloatVector) */
    public static  double weightedSum( long[] values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],io.deephaven.vector.IntVector) */
    public static  double weightedSum( long[] values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],io.deephaven.vector.LongVector) */
    public static  double weightedSum( long[] values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(long[],io.deephaven.vector.ShortVector) */
    public static  double weightedSum( long[] values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],double[]) */
    public static  double weightedSum( short[] values, double[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],float[]) */
    public static  double weightedSum( short[] values, float[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],int[]) */
    public static  double weightedSum( short[] values, int[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],long[]) */
    public static  double weightedSum( short[] values, long[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],short[]) */
    public static  double weightedSum( short[] values, short[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( short[] values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],io.deephaven.vector.FloatVector) */
    public static  double weightedSum( short[] values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],io.deephaven.vector.IntVector) */
    public static  double weightedSum( short[] values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],io.deephaven.vector.LongVector) */
    public static  double weightedSum( short[] values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(short[],io.deephaven.vector.ShortVector) */
    public static  double weightedSum( short[] values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,byte[]) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, byte[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,double[]) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, double[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,float[]) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, float[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,int[]) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, int[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,long[]) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, long[] weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,io.deephaven.vector.FloatVector) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,io.deephaven.vector.IntVector) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#weightedSum(io.deephaven.vector.ByteVector,io.deephaven.vector.LongVector) */
    public static  double weightedSum( io.deephaven.vector.ByteVector values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,double[]) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, double[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,float[]) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, float[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,int[]) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, int[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,long[]) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, long[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,short[]) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, short[] weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,io.deephaven.vector.FloatVector) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,io.deephaven.vector.IntVector) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,io.deephaven.vector.LongVector) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#weightedSum(io.deephaven.vector.DoubleVector,io.deephaven.vector.ShortVector) */
    public static  double weightedSum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,double[]) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, double[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,float[]) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, float[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,int[]) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, int[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,long[]) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, long[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,short[]) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, short[] weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,io.deephaven.vector.IntVector) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,io.deephaven.vector.LongVector) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#weightedSum(io.deephaven.vector.FloatVector,io.deephaven.vector.ShortVector) */
    public static  double weightedSum( io.deephaven.vector.FloatVector values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,double[]) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, double[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,float[]) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, float[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,int[]) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, int[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,long[]) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, long[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,short[]) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, short[] weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,io.deephaven.vector.FloatVector) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,io.deephaven.vector.LongVector) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#weightedSum(io.deephaven.vector.IntVector,io.deephaven.vector.ShortVector) */
    public static  double weightedSum( io.deephaven.vector.IntVector values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,double[]) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, double[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,float[]) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, float[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,int[]) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, int[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,long[]) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, long[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,short[]) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, short[] weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,io.deephaven.vector.FloatVector) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,io.deephaven.vector.IntVector) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#weightedSum(io.deephaven.vector.LongVector,io.deephaven.vector.ShortVector) */
    public static  double weightedSum( io.deephaven.vector.LongVector values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,double[]) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, double[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,float[]) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, float[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,int[]) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, int[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,long[]) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, long[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,short[]) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, short[] weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,io.deephaven.vector.DoubleVector) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,io.deephaven.vector.FloatVector) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,io.deephaven.vector.IntVector) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,io.deephaven.vector.LongVector) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#weightedSum(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double weightedSum( io.deephaven.vector.ShortVector values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.weightedSum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],byte[]) */
    public static  double wstd( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],double[]) */
    public static  double wstd( byte[] values, double[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],float[]) */
    public static  double wstd( byte[] values, float[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],int[]) */
    public static  double wstd( byte[] values, int[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],long[]) */
    public static  double wstd( byte[] values, long[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],io.deephaven.vector.ByteVector) */
    public static  double wstd( byte[] values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],io.deephaven.vector.DoubleVector) */
    public static  double wstd( byte[] values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],io.deephaven.vector.FloatVector) */
    public static  double wstd( byte[] values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],io.deephaven.vector.IntVector) */
    public static  double wstd( byte[] values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(byte[],io.deephaven.vector.LongVector) */
    public static  double wstd( byte[] values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],double[]) */
    public static  double wstd( double[] values, double[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],float[]) */
    public static  double wstd( double[] values, float[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],int[]) */
    public static  double wstd( double[] values, int[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],long[]) */
    public static  double wstd( double[] values, long[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],short[]) */
    public static  double wstd( double[] values, short[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],io.deephaven.vector.DoubleVector) */
    public static  double wstd( double[] values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],io.deephaven.vector.FloatVector) */
    public static  double wstd( double[] values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],io.deephaven.vector.IntVector) */
    public static  double wstd( double[] values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],io.deephaven.vector.LongVector) */
    public static  double wstd( double[] values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(double[],io.deephaven.vector.ShortVector) */
    public static  double wstd( double[] values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],double[]) */
    public static  double wstd( float[] values, double[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],float[]) */
    public static  double wstd( float[] values, float[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],int[]) */
    public static  double wstd( float[] values, int[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],long[]) */
    public static  double wstd( float[] values, long[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],short[]) */
    public static  double wstd( float[] values, short[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],io.deephaven.vector.DoubleVector) */
    public static  double wstd( float[] values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],io.deephaven.vector.FloatVector) */
    public static  double wstd( float[] values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],io.deephaven.vector.IntVector) */
    public static  double wstd( float[] values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],io.deephaven.vector.LongVector) */
    public static  double wstd( float[] values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(float[],io.deephaven.vector.ShortVector) */
    public static  double wstd( float[] values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],double[]) */
    public static  double wstd( int[] values, double[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],float[]) */
    public static  double wstd( int[] values, float[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],int[]) */
    public static  double wstd( int[] values, int[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],long[]) */
    public static  double wstd( int[] values, long[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],short[]) */
    public static  double wstd( int[] values, short[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],io.deephaven.vector.DoubleVector) */
    public static  double wstd( int[] values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],io.deephaven.vector.FloatVector) */
    public static  double wstd( int[] values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],io.deephaven.vector.IntVector) */
    public static  double wstd( int[] values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],io.deephaven.vector.LongVector) */
    public static  double wstd( int[] values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(int[],io.deephaven.vector.ShortVector) */
    public static  double wstd( int[] values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],double[]) */
    public static  double wstd( long[] values, double[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],float[]) */
    public static  double wstd( long[] values, float[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],int[]) */
    public static  double wstd( long[] values, int[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],long[]) */
    public static  double wstd( long[] values, long[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],short[]) */
    public static  double wstd( long[] values, short[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],io.deephaven.vector.DoubleVector) */
    public static  double wstd( long[] values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],io.deephaven.vector.FloatVector) */
    public static  double wstd( long[] values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],io.deephaven.vector.IntVector) */
    public static  double wstd( long[] values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],io.deephaven.vector.LongVector) */
    public static  double wstd( long[] values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(long[],io.deephaven.vector.ShortVector) */
    public static  double wstd( long[] values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],double[]) */
    public static  double wstd( short[] values, double[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],float[]) */
    public static  double wstd( short[] values, float[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],int[]) */
    public static  double wstd( short[] values, int[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],long[]) */
    public static  double wstd( short[] values, long[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],short[]) */
    public static  double wstd( short[] values, short[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],io.deephaven.vector.DoubleVector) */
    public static  double wstd( short[] values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],io.deephaven.vector.FloatVector) */
    public static  double wstd( short[] values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],io.deephaven.vector.IntVector) */
    public static  double wstd( short[] values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],io.deephaven.vector.LongVector) */
    public static  double wstd( short[] values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(short[],io.deephaven.vector.ShortVector) */
    public static  double wstd( short[] values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,byte[]) */
    public static  double wstd( io.deephaven.vector.ByteVector values, byte[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,double[]) */
    public static  double wstd( io.deephaven.vector.ByteVector values, double[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,float[]) */
    public static  double wstd( io.deephaven.vector.ByteVector values, float[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,int[]) */
    public static  double wstd( io.deephaven.vector.ByteVector values, int[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,long[]) */
    public static  double wstd( io.deephaven.vector.ByteVector values, long[] weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double wstd( io.deephaven.vector.ByteVector values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,io.deephaven.vector.DoubleVector) */
    public static  double wstd( io.deephaven.vector.ByteVector values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,io.deephaven.vector.FloatVector) */
    public static  double wstd( io.deephaven.vector.ByteVector values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,io.deephaven.vector.IntVector) */
    public static  double wstd( io.deephaven.vector.ByteVector values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wstd(io.deephaven.vector.ByteVector,io.deephaven.vector.LongVector) */
    public static  double wstd( io.deephaven.vector.ByteVector values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,double[]) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, double[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,float[]) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, float[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,int[]) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, int[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,long[]) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, long[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,short[]) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, short[] weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,io.deephaven.vector.FloatVector) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,io.deephaven.vector.IntVector) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,io.deephaven.vector.LongVector) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wstd(io.deephaven.vector.DoubleVector,io.deephaven.vector.ShortVector) */
    public static  double wstd( io.deephaven.vector.DoubleVector values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,double[]) */
    public static  double wstd( io.deephaven.vector.FloatVector values, double[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,float[]) */
    public static  double wstd( io.deephaven.vector.FloatVector values, float[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,int[]) */
    public static  double wstd( io.deephaven.vector.FloatVector values, int[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,long[]) */
    public static  double wstd( io.deephaven.vector.FloatVector values, long[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,short[]) */
    public static  double wstd( io.deephaven.vector.FloatVector values, short[] weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,io.deephaven.vector.DoubleVector) */
    public static  double wstd( io.deephaven.vector.FloatVector values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double wstd( io.deephaven.vector.FloatVector values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,io.deephaven.vector.IntVector) */
    public static  double wstd( io.deephaven.vector.FloatVector values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,io.deephaven.vector.LongVector) */
    public static  double wstd( io.deephaven.vector.FloatVector values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wstd(io.deephaven.vector.FloatVector,io.deephaven.vector.ShortVector) */
    public static  double wstd( io.deephaven.vector.FloatVector values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,double[]) */
    public static  double wstd( io.deephaven.vector.IntVector values, double[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,float[]) */
    public static  double wstd( io.deephaven.vector.IntVector values, float[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,int[]) */
    public static  double wstd( io.deephaven.vector.IntVector values, int[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,long[]) */
    public static  double wstd( io.deephaven.vector.IntVector values, long[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,short[]) */
    public static  double wstd( io.deephaven.vector.IntVector values, short[] weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,io.deephaven.vector.DoubleVector) */
    public static  double wstd( io.deephaven.vector.IntVector values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,io.deephaven.vector.FloatVector) */
    public static  double wstd( io.deephaven.vector.IntVector values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double wstd( io.deephaven.vector.IntVector values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,io.deephaven.vector.LongVector) */
    public static  double wstd( io.deephaven.vector.IntVector values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wstd(io.deephaven.vector.IntVector,io.deephaven.vector.ShortVector) */
    public static  double wstd( io.deephaven.vector.IntVector values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,double[]) */
    public static  double wstd( io.deephaven.vector.LongVector values, double[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,float[]) */
    public static  double wstd( io.deephaven.vector.LongVector values, float[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,int[]) */
    public static  double wstd( io.deephaven.vector.LongVector values, int[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,long[]) */
    public static  double wstd( io.deephaven.vector.LongVector values, long[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,short[]) */
    public static  double wstd( io.deephaven.vector.LongVector values, short[] weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,io.deephaven.vector.DoubleVector) */
    public static  double wstd( io.deephaven.vector.LongVector values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,io.deephaven.vector.FloatVector) */
    public static  double wstd( io.deephaven.vector.LongVector values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,io.deephaven.vector.IntVector) */
    public static  double wstd( io.deephaven.vector.LongVector values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double wstd( io.deephaven.vector.LongVector values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wstd(io.deephaven.vector.LongVector,io.deephaven.vector.ShortVector) */
    public static  double wstd( io.deephaven.vector.LongVector values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,double[]) */
    public static  double wstd( io.deephaven.vector.ShortVector values, double[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,float[]) */
    public static  double wstd( io.deephaven.vector.ShortVector values, float[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,int[]) */
    public static  double wstd( io.deephaven.vector.ShortVector values, int[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,long[]) */
    public static  double wstd( io.deephaven.vector.ShortVector values, long[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,short[]) */
    public static  double wstd( io.deephaven.vector.ShortVector values, short[] weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,io.deephaven.vector.DoubleVector) */
    public static  double wstd( io.deephaven.vector.ShortVector values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,io.deephaven.vector.FloatVector) */
    public static  double wstd( io.deephaven.vector.ShortVector values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,io.deephaven.vector.IntVector) */
    public static  double wstd( io.deephaven.vector.ShortVector values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,io.deephaven.vector.LongVector) */
    public static  double wstd( io.deephaven.vector.ShortVector values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wstd(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double wstd( io.deephaven.vector.ShortVector values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wstd( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],byte[]) */
    public static  double wste( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],double[]) */
    public static  double wste( byte[] values, double[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],float[]) */
    public static  double wste( byte[] values, float[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],int[]) */
    public static  double wste( byte[] values, int[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],long[]) */
    public static  double wste( byte[] values, long[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],io.deephaven.vector.ByteVector) */
    public static  double wste( byte[] values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],io.deephaven.vector.DoubleVector) */
    public static  double wste( byte[] values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],io.deephaven.vector.FloatVector) */
    public static  double wste( byte[] values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],io.deephaven.vector.IntVector) */
    public static  double wste( byte[] values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(byte[],io.deephaven.vector.LongVector) */
    public static  double wste( byte[] values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],double[]) */
    public static  double wste( double[] values, double[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],float[]) */
    public static  double wste( double[] values, float[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],int[]) */
    public static  double wste( double[] values, int[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],long[]) */
    public static  double wste( double[] values, long[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],short[]) */
    public static  double wste( double[] values, short[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],io.deephaven.vector.DoubleVector) */
    public static  double wste( double[] values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],io.deephaven.vector.FloatVector) */
    public static  double wste( double[] values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],io.deephaven.vector.IntVector) */
    public static  double wste( double[] values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],io.deephaven.vector.LongVector) */
    public static  double wste( double[] values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(double[],io.deephaven.vector.ShortVector) */
    public static  double wste( double[] values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],double[]) */
    public static  double wste( float[] values, double[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],float[]) */
    public static  double wste( float[] values, float[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],int[]) */
    public static  double wste( float[] values, int[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],long[]) */
    public static  double wste( float[] values, long[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],short[]) */
    public static  double wste( float[] values, short[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],io.deephaven.vector.DoubleVector) */
    public static  double wste( float[] values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],io.deephaven.vector.FloatVector) */
    public static  double wste( float[] values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],io.deephaven.vector.IntVector) */
    public static  double wste( float[] values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],io.deephaven.vector.LongVector) */
    public static  double wste( float[] values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(float[],io.deephaven.vector.ShortVector) */
    public static  double wste( float[] values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],double[]) */
    public static  double wste( int[] values, double[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],float[]) */
    public static  double wste( int[] values, float[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],int[]) */
    public static  double wste( int[] values, int[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],long[]) */
    public static  double wste( int[] values, long[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],short[]) */
    public static  double wste( int[] values, short[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],io.deephaven.vector.DoubleVector) */
    public static  double wste( int[] values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],io.deephaven.vector.FloatVector) */
    public static  double wste( int[] values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],io.deephaven.vector.IntVector) */
    public static  double wste( int[] values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],io.deephaven.vector.LongVector) */
    public static  double wste( int[] values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(int[],io.deephaven.vector.ShortVector) */
    public static  double wste( int[] values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],double[]) */
    public static  double wste( long[] values, double[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],float[]) */
    public static  double wste( long[] values, float[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],int[]) */
    public static  double wste( long[] values, int[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],long[]) */
    public static  double wste( long[] values, long[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],short[]) */
    public static  double wste( long[] values, short[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],io.deephaven.vector.DoubleVector) */
    public static  double wste( long[] values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],io.deephaven.vector.FloatVector) */
    public static  double wste( long[] values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],io.deephaven.vector.IntVector) */
    public static  double wste( long[] values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],io.deephaven.vector.LongVector) */
    public static  double wste( long[] values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(long[],io.deephaven.vector.ShortVector) */
    public static  double wste( long[] values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],double[]) */
    public static  double wste( short[] values, double[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],float[]) */
    public static  double wste( short[] values, float[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],int[]) */
    public static  double wste( short[] values, int[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],long[]) */
    public static  double wste( short[] values, long[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],short[]) */
    public static  double wste( short[] values, short[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],io.deephaven.vector.DoubleVector) */
    public static  double wste( short[] values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],io.deephaven.vector.FloatVector) */
    public static  double wste( short[] values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],io.deephaven.vector.IntVector) */
    public static  double wste( short[] values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],io.deephaven.vector.LongVector) */
    public static  double wste( short[] values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(short[],io.deephaven.vector.ShortVector) */
    public static  double wste( short[] values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,byte[]) */
    public static  double wste( io.deephaven.vector.ByteVector values, byte[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,double[]) */
    public static  double wste( io.deephaven.vector.ByteVector values, double[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,float[]) */
    public static  double wste( io.deephaven.vector.ByteVector values, float[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,int[]) */
    public static  double wste( io.deephaven.vector.ByteVector values, int[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,long[]) */
    public static  double wste( io.deephaven.vector.ByteVector values, long[] weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double wste( io.deephaven.vector.ByteVector values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,io.deephaven.vector.DoubleVector) */
    public static  double wste( io.deephaven.vector.ByteVector values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,io.deephaven.vector.FloatVector) */
    public static  double wste( io.deephaven.vector.ByteVector values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,io.deephaven.vector.IntVector) */
    public static  double wste( io.deephaven.vector.ByteVector values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wste(io.deephaven.vector.ByteVector,io.deephaven.vector.LongVector) */
    public static  double wste( io.deephaven.vector.ByteVector values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,double[]) */
    public static  double wste( io.deephaven.vector.DoubleVector values, double[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,float[]) */
    public static  double wste( io.deephaven.vector.DoubleVector values, float[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,int[]) */
    public static  double wste( io.deephaven.vector.DoubleVector values, int[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,long[]) */
    public static  double wste( io.deephaven.vector.DoubleVector values, long[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,short[]) */
    public static  double wste( io.deephaven.vector.DoubleVector values, short[] weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double wste( io.deephaven.vector.DoubleVector values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,io.deephaven.vector.FloatVector) */
    public static  double wste( io.deephaven.vector.DoubleVector values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,io.deephaven.vector.IntVector) */
    public static  double wste( io.deephaven.vector.DoubleVector values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,io.deephaven.vector.LongVector) */
    public static  double wste( io.deephaven.vector.DoubleVector values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wste(io.deephaven.vector.DoubleVector,io.deephaven.vector.ShortVector) */
    public static  double wste( io.deephaven.vector.DoubleVector values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,double[]) */
    public static  double wste( io.deephaven.vector.FloatVector values, double[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,float[]) */
    public static  double wste( io.deephaven.vector.FloatVector values, float[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,int[]) */
    public static  double wste( io.deephaven.vector.FloatVector values, int[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,long[]) */
    public static  double wste( io.deephaven.vector.FloatVector values, long[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,short[]) */
    public static  double wste( io.deephaven.vector.FloatVector values, short[] weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,io.deephaven.vector.DoubleVector) */
    public static  double wste( io.deephaven.vector.FloatVector values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double wste( io.deephaven.vector.FloatVector values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,io.deephaven.vector.IntVector) */
    public static  double wste( io.deephaven.vector.FloatVector values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,io.deephaven.vector.LongVector) */
    public static  double wste( io.deephaven.vector.FloatVector values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wste(io.deephaven.vector.FloatVector,io.deephaven.vector.ShortVector) */
    public static  double wste( io.deephaven.vector.FloatVector values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,double[]) */
    public static  double wste( io.deephaven.vector.IntVector values, double[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,float[]) */
    public static  double wste( io.deephaven.vector.IntVector values, float[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,int[]) */
    public static  double wste( io.deephaven.vector.IntVector values, int[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,long[]) */
    public static  double wste( io.deephaven.vector.IntVector values, long[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,short[]) */
    public static  double wste( io.deephaven.vector.IntVector values, short[] weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,io.deephaven.vector.DoubleVector) */
    public static  double wste( io.deephaven.vector.IntVector values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,io.deephaven.vector.FloatVector) */
    public static  double wste( io.deephaven.vector.IntVector values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double wste( io.deephaven.vector.IntVector values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,io.deephaven.vector.LongVector) */
    public static  double wste( io.deephaven.vector.IntVector values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wste(io.deephaven.vector.IntVector,io.deephaven.vector.ShortVector) */
    public static  double wste( io.deephaven.vector.IntVector values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,double[]) */
    public static  double wste( io.deephaven.vector.LongVector values, double[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,float[]) */
    public static  double wste( io.deephaven.vector.LongVector values, float[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,int[]) */
    public static  double wste( io.deephaven.vector.LongVector values, int[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,long[]) */
    public static  double wste( io.deephaven.vector.LongVector values, long[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,short[]) */
    public static  double wste( io.deephaven.vector.LongVector values, short[] weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,io.deephaven.vector.DoubleVector) */
    public static  double wste( io.deephaven.vector.LongVector values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,io.deephaven.vector.FloatVector) */
    public static  double wste( io.deephaven.vector.LongVector values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,io.deephaven.vector.IntVector) */
    public static  double wste( io.deephaven.vector.LongVector values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double wste( io.deephaven.vector.LongVector values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wste(io.deephaven.vector.LongVector,io.deephaven.vector.ShortVector) */
    public static  double wste( io.deephaven.vector.LongVector values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,double[]) */
    public static  double wste( io.deephaven.vector.ShortVector values, double[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,float[]) */
    public static  double wste( io.deephaven.vector.ShortVector values, float[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,int[]) */
    public static  double wste( io.deephaven.vector.ShortVector values, int[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,long[]) */
    public static  double wste( io.deephaven.vector.ShortVector values, long[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,short[]) */
    public static  double wste( io.deephaven.vector.ShortVector values, short[] weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,io.deephaven.vector.DoubleVector) */
    public static  double wste( io.deephaven.vector.ShortVector values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,io.deephaven.vector.FloatVector) */
    public static  double wste( io.deephaven.vector.ShortVector values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,io.deephaven.vector.IntVector) */
    public static  double wste( io.deephaven.vector.ShortVector values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,io.deephaven.vector.LongVector) */
    public static  double wste( io.deephaven.vector.ShortVector values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wste(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double wste( io.deephaven.vector.ShortVector values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wste( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],byte[]) */
    public static  double wsum( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],double[]) */
    public static  double wsum( byte[] values, double[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],float[]) */
    public static  double wsum( byte[] values, float[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],int[]) */
    public static  double wsum( byte[] values, int[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],long[]) */
    public static  double wsum( byte[] values, long[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],io.deephaven.vector.ByteVector) */
    public static  double wsum( byte[] values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],io.deephaven.vector.DoubleVector) */
    public static  double wsum( byte[] values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],io.deephaven.vector.FloatVector) */
    public static  double wsum( byte[] values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],io.deephaven.vector.IntVector) */
    public static  double wsum( byte[] values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(byte[],io.deephaven.vector.LongVector) */
    public static  double wsum( byte[] values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],double[]) */
    public static  double wsum( double[] values, double[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],float[]) */
    public static  double wsum( double[] values, float[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],int[]) */
    public static  double wsum( double[] values, int[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],long[]) */
    public static  double wsum( double[] values, long[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],short[]) */
    public static  double wsum( double[] values, short[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],io.deephaven.vector.DoubleVector) */
    public static  double wsum( double[] values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],io.deephaven.vector.FloatVector) */
    public static  double wsum( double[] values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],io.deephaven.vector.IntVector) */
    public static  double wsum( double[] values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],io.deephaven.vector.LongVector) */
    public static  double wsum( double[] values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(double[],io.deephaven.vector.ShortVector) */
    public static  double wsum( double[] values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],double[]) */
    public static  double wsum( float[] values, double[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],float[]) */
    public static  double wsum( float[] values, float[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],int[]) */
    public static  double wsum( float[] values, int[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],long[]) */
    public static  double wsum( float[] values, long[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],short[]) */
    public static  double wsum( float[] values, short[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],io.deephaven.vector.DoubleVector) */
    public static  double wsum( float[] values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],io.deephaven.vector.FloatVector) */
    public static  double wsum( float[] values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],io.deephaven.vector.IntVector) */
    public static  double wsum( float[] values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],io.deephaven.vector.LongVector) */
    public static  double wsum( float[] values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(float[],io.deephaven.vector.ShortVector) */
    public static  double wsum( float[] values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],double[]) */
    public static  double wsum( int[] values, double[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],float[]) */
    public static  double wsum( int[] values, float[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],int[]) */
    public static  double wsum( int[] values, int[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],long[]) */
    public static  double wsum( int[] values, long[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],short[]) */
    public static  double wsum( int[] values, short[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],io.deephaven.vector.DoubleVector) */
    public static  double wsum( int[] values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],io.deephaven.vector.FloatVector) */
    public static  double wsum( int[] values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],io.deephaven.vector.IntVector) */
    public static  double wsum( int[] values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],io.deephaven.vector.LongVector) */
    public static  double wsum( int[] values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(int[],io.deephaven.vector.ShortVector) */
    public static  double wsum( int[] values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],double[]) */
    public static  double wsum( long[] values, double[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],float[]) */
    public static  double wsum( long[] values, float[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],int[]) */
    public static  double wsum( long[] values, int[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],long[]) */
    public static  double wsum( long[] values, long[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],short[]) */
    public static  double wsum( long[] values, short[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],io.deephaven.vector.DoubleVector) */
    public static  double wsum( long[] values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],io.deephaven.vector.FloatVector) */
    public static  double wsum( long[] values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],io.deephaven.vector.IntVector) */
    public static  double wsum( long[] values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],io.deephaven.vector.LongVector) */
    public static  double wsum( long[] values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(long[],io.deephaven.vector.ShortVector) */
    public static  double wsum( long[] values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],double[]) */
    public static  double wsum( short[] values, double[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],float[]) */
    public static  double wsum( short[] values, float[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],int[]) */
    public static  double wsum( short[] values, int[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],long[]) */
    public static  double wsum( short[] values, long[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],short[]) */
    public static  double wsum( short[] values, short[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],io.deephaven.vector.DoubleVector) */
    public static  double wsum( short[] values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],io.deephaven.vector.FloatVector) */
    public static  double wsum( short[] values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],io.deephaven.vector.IntVector) */
    public static  double wsum( short[] values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],io.deephaven.vector.LongVector) */
    public static  double wsum( short[] values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(short[],io.deephaven.vector.ShortVector) */
    public static  double wsum( short[] values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,byte[]) */
    public static  double wsum( io.deephaven.vector.ByteVector values, byte[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,double[]) */
    public static  double wsum( io.deephaven.vector.ByteVector values, double[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,float[]) */
    public static  double wsum( io.deephaven.vector.ByteVector values, float[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,int[]) */
    public static  double wsum( io.deephaven.vector.ByteVector values, int[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,long[]) */
    public static  double wsum( io.deephaven.vector.ByteVector values, long[] weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double wsum( io.deephaven.vector.ByteVector values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,io.deephaven.vector.DoubleVector) */
    public static  double wsum( io.deephaven.vector.ByteVector values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,io.deephaven.vector.FloatVector) */
    public static  double wsum( io.deephaven.vector.ByteVector values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,io.deephaven.vector.IntVector) */
    public static  double wsum( io.deephaven.vector.ByteVector values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wsum(io.deephaven.vector.ByteVector,io.deephaven.vector.LongVector) */
    public static  double wsum( io.deephaven.vector.ByteVector values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,double[]) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, double[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,float[]) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, float[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,int[]) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, int[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,long[]) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, long[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,short[]) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, short[] weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,io.deephaven.vector.FloatVector) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,io.deephaven.vector.IntVector) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,io.deephaven.vector.LongVector) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wsum(io.deephaven.vector.DoubleVector,io.deephaven.vector.ShortVector) */
    public static  double wsum( io.deephaven.vector.DoubleVector values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,double[]) */
    public static  double wsum( io.deephaven.vector.FloatVector values, double[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,float[]) */
    public static  double wsum( io.deephaven.vector.FloatVector values, float[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,int[]) */
    public static  double wsum( io.deephaven.vector.FloatVector values, int[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,long[]) */
    public static  double wsum( io.deephaven.vector.FloatVector values, long[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,short[]) */
    public static  double wsum( io.deephaven.vector.FloatVector values, short[] weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,io.deephaven.vector.DoubleVector) */
    public static  double wsum( io.deephaven.vector.FloatVector values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double wsum( io.deephaven.vector.FloatVector values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,io.deephaven.vector.IntVector) */
    public static  double wsum( io.deephaven.vector.FloatVector values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,io.deephaven.vector.LongVector) */
    public static  double wsum( io.deephaven.vector.FloatVector values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wsum(io.deephaven.vector.FloatVector,io.deephaven.vector.ShortVector) */
    public static  double wsum( io.deephaven.vector.FloatVector values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,double[]) */
    public static  double wsum( io.deephaven.vector.IntVector values, double[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,float[]) */
    public static  double wsum( io.deephaven.vector.IntVector values, float[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,int[]) */
    public static  double wsum( io.deephaven.vector.IntVector values, int[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,long[]) */
    public static  double wsum( io.deephaven.vector.IntVector values, long[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,short[]) */
    public static  double wsum( io.deephaven.vector.IntVector values, short[] weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,io.deephaven.vector.DoubleVector) */
    public static  double wsum( io.deephaven.vector.IntVector values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,io.deephaven.vector.FloatVector) */
    public static  double wsum( io.deephaven.vector.IntVector values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double wsum( io.deephaven.vector.IntVector values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,io.deephaven.vector.LongVector) */
    public static  double wsum( io.deephaven.vector.IntVector values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wsum(io.deephaven.vector.IntVector,io.deephaven.vector.ShortVector) */
    public static  double wsum( io.deephaven.vector.IntVector values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,double[]) */
    public static  double wsum( io.deephaven.vector.LongVector values, double[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,float[]) */
    public static  double wsum( io.deephaven.vector.LongVector values, float[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,int[]) */
    public static  double wsum( io.deephaven.vector.LongVector values, int[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,long[]) */
    public static  double wsum( io.deephaven.vector.LongVector values, long[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,short[]) */
    public static  double wsum( io.deephaven.vector.LongVector values, short[] weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,io.deephaven.vector.DoubleVector) */
    public static  double wsum( io.deephaven.vector.LongVector values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,io.deephaven.vector.FloatVector) */
    public static  double wsum( io.deephaven.vector.LongVector values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,io.deephaven.vector.IntVector) */
    public static  double wsum( io.deephaven.vector.LongVector values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double wsum( io.deephaven.vector.LongVector values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wsum(io.deephaven.vector.LongVector,io.deephaven.vector.ShortVector) */
    public static  double wsum( io.deephaven.vector.LongVector values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,double[]) */
    public static  double wsum( io.deephaven.vector.ShortVector values, double[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,float[]) */
    public static  double wsum( io.deephaven.vector.ShortVector values, float[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,int[]) */
    public static  double wsum( io.deephaven.vector.ShortVector values, int[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,long[]) */
    public static  double wsum( io.deephaven.vector.ShortVector values, long[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,short[]) */
    public static  double wsum( io.deephaven.vector.ShortVector values, short[] weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,io.deephaven.vector.DoubleVector) */
    public static  double wsum( io.deephaven.vector.ShortVector values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,io.deephaven.vector.FloatVector) */
    public static  double wsum( io.deephaven.vector.ShortVector values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,io.deephaven.vector.IntVector) */
    public static  double wsum( io.deephaven.vector.ShortVector values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,io.deephaven.vector.LongVector) */
    public static  double wsum( io.deephaven.vector.ShortVector values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wsum(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double wsum( io.deephaven.vector.ShortVector values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wsum( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],byte[]) */
    public static  double wtstat( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],double[]) */
    public static  double wtstat( byte[] values, double[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],float[]) */
    public static  double wtstat( byte[] values, float[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],int[]) */
    public static  double wtstat( byte[] values, int[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],long[]) */
    public static  double wtstat( byte[] values, long[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],io.deephaven.vector.ByteVector) */
    public static  double wtstat( byte[] values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],io.deephaven.vector.DoubleVector) */
    public static  double wtstat( byte[] values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],io.deephaven.vector.FloatVector) */
    public static  double wtstat( byte[] values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],io.deephaven.vector.IntVector) */
    public static  double wtstat( byte[] values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(byte[],io.deephaven.vector.LongVector) */
    public static  double wtstat( byte[] values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],double[]) */
    public static  double wtstat( double[] values, double[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],float[]) */
    public static  double wtstat( double[] values, float[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],int[]) */
    public static  double wtstat( double[] values, int[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],long[]) */
    public static  double wtstat( double[] values, long[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],short[]) */
    public static  double wtstat( double[] values, short[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],io.deephaven.vector.DoubleVector) */
    public static  double wtstat( double[] values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],io.deephaven.vector.FloatVector) */
    public static  double wtstat( double[] values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],io.deephaven.vector.IntVector) */
    public static  double wtstat( double[] values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],io.deephaven.vector.LongVector) */
    public static  double wtstat( double[] values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(double[],io.deephaven.vector.ShortVector) */
    public static  double wtstat( double[] values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],double[]) */
    public static  double wtstat( float[] values, double[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],float[]) */
    public static  double wtstat( float[] values, float[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],int[]) */
    public static  double wtstat( float[] values, int[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],long[]) */
    public static  double wtstat( float[] values, long[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],short[]) */
    public static  double wtstat( float[] values, short[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],io.deephaven.vector.DoubleVector) */
    public static  double wtstat( float[] values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],io.deephaven.vector.FloatVector) */
    public static  double wtstat( float[] values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],io.deephaven.vector.IntVector) */
    public static  double wtstat( float[] values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],io.deephaven.vector.LongVector) */
    public static  double wtstat( float[] values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(float[],io.deephaven.vector.ShortVector) */
    public static  double wtstat( float[] values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],double[]) */
    public static  double wtstat( int[] values, double[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],float[]) */
    public static  double wtstat( int[] values, float[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],int[]) */
    public static  double wtstat( int[] values, int[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],long[]) */
    public static  double wtstat( int[] values, long[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],short[]) */
    public static  double wtstat( int[] values, short[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],io.deephaven.vector.DoubleVector) */
    public static  double wtstat( int[] values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],io.deephaven.vector.FloatVector) */
    public static  double wtstat( int[] values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],io.deephaven.vector.IntVector) */
    public static  double wtstat( int[] values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],io.deephaven.vector.LongVector) */
    public static  double wtstat( int[] values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(int[],io.deephaven.vector.ShortVector) */
    public static  double wtstat( int[] values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],double[]) */
    public static  double wtstat( long[] values, double[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],float[]) */
    public static  double wtstat( long[] values, float[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],int[]) */
    public static  double wtstat( long[] values, int[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],long[]) */
    public static  double wtstat( long[] values, long[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],short[]) */
    public static  double wtstat( long[] values, short[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],io.deephaven.vector.DoubleVector) */
    public static  double wtstat( long[] values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],io.deephaven.vector.FloatVector) */
    public static  double wtstat( long[] values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],io.deephaven.vector.IntVector) */
    public static  double wtstat( long[] values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],io.deephaven.vector.LongVector) */
    public static  double wtstat( long[] values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(long[],io.deephaven.vector.ShortVector) */
    public static  double wtstat( long[] values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],double[]) */
    public static  double wtstat( short[] values, double[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],float[]) */
    public static  double wtstat( short[] values, float[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],int[]) */
    public static  double wtstat( short[] values, int[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],long[]) */
    public static  double wtstat( short[] values, long[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],short[]) */
    public static  double wtstat( short[] values, short[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],io.deephaven.vector.DoubleVector) */
    public static  double wtstat( short[] values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],io.deephaven.vector.FloatVector) */
    public static  double wtstat( short[] values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],io.deephaven.vector.IntVector) */
    public static  double wtstat( short[] values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],io.deephaven.vector.LongVector) */
    public static  double wtstat( short[] values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(short[],io.deephaven.vector.ShortVector) */
    public static  double wtstat( short[] values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,byte[]) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, byte[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,double[]) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, double[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,float[]) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, float[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,int[]) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, int[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,long[]) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, long[] weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,io.deephaven.vector.DoubleVector) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,io.deephaven.vector.FloatVector) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,io.deephaven.vector.IntVector) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wtstat(io.deephaven.vector.ByteVector,io.deephaven.vector.LongVector) */
    public static  double wtstat( io.deephaven.vector.ByteVector values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,double[]) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, double[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,float[]) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, float[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,int[]) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, int[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,long[]) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, long[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,short[]) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, short[] weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,io.deephaven.vector.FloatVector) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,io.deephaven.vector.IntVector) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,io.deephaven.vector.LongVector) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wtstat(io.deephaven.vector.DoubleVector,io.deephaven.vector.ShortVector) */
    public static  double wtstat( io.deephaven.vector.DoubleVector values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,double[]) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, double[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,float[]) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, float[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,int[]) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, int[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,long[]) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, long[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,short[]) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, short[] weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,io.deephaven.vector.DoubleVector) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,io.deephaven.vector.IntVector) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,io.deephaven.vector.LongVector) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wtstat(io.deephaven.vector.FloatVector,io.deephaven.vector.ShortVector) */
    public static  double wtstat( io.deephaven.vector.FloatVector values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,double[]) */
    public static  double wtstat( io.deephaven.vector.IntVector values, double[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,float[]) */
    public static  double wtstat( io.deephaven.vector.IntVector values, float[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,int[]) */
    public static  double wtstat( io.deephaven.vector.IntVector values, int[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,long[]) */
    public static  double wtstat( io.deephaven.vector.IntVector values, long[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,short[]) */
    public static  double wtstat( io.deephaven.vector.IntVector values, short[] weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,io.deephaven.vector.DoubleVector) */
    public static  double wtstat( io.deephaven.vector.IntVector values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,io.deephaven.vector.FloatVector) */
    public static  double wtstat( io.deephaven.vector.IntVector values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double wtstat( io.deephaven.vector.IntVector values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,io.deephaven.vector.LongVector) */
    public static  double wtstat( io.deephaven.vector.IntVector values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wtstat(io.deephaven.vector.IntVector,io.deephaven.vector.ShortVector) */
    public static  double wtstat( io.deephaven.vector.IntVector values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,double[]) */
    public static  double wtstat( io.deephaven.vector.LongVector values, double[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,float[]) */
    public static  double wtstat( io.deephaven.vector.LongVector values, float[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,int[]) */
    public static  double wtstat( io.deephaven.vector.LongVector values, int[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,long[]) */
    public static  double wtstat( io.deephaven.vector.LongVector values, long[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,short[]) */
    public static  double wtstat( io.deephaven.vector.LongVector values, short[] weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,io.deephaven.vector.DoubleVector) */
    public static  double wtstat( io.deephaven.vector.LongVector values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,io.deephaven.vector.FloatVector) */
    public static  double wtstat( io.deephaven.vector.LongVector values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,io.deephaven.vector.IntVector) */
    public static  double wtstat( io.deephaven.vector.LongVector values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double wtstat( io.deephaven.vector.LongVector values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wtstat(io.deephaven.vector.LongVector,io.deephaven.vector.ShortVector) */
    public static  double wtstat( io.deephaven.vector.LongVector values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,double[]) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, double[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,float[]) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, float[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,int[]) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, int[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,long[]) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, long[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,short[]) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, short[] weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,io.deephaven.vector.DoubleVector) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,io.deephaven.vector.FloatVector) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,io.deephaven.vector.IntVector) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,io.deephaven.vector.LongVector) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wtstat(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double wtstat( io.deephaven.vector.ShortVector values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wtstat( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],byte[]) */
    public static  double wvar( byte[] values, byte[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],double[]) */
    public static  double wvar( byte[] values, double[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],float[]) */
    public static  double wvar( byte[] values, float[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],int[]) */
    public static  double wvar( byte[] values, int[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],long[]) */
    public static  double wvar( byte[] values, long[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],io.deephaven.vector.ByteVector) */
    public static  double wvar( byte[] values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],io.deephaven.vector.DoubleVector) */
    public static  double wvar( byte[] values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],io.deephaven.vector.FloatVector) */
    public static  double wvar( byte[] values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],io.deephaven.vector.IntVector) */
    public static  double wvar( byte[] values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(byte[],io.deephaven.vector.LongVector) */
    public static  double wvar( byte[] values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],double[]) */
    public static  double wvar( double[] values, double[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],float[]) */
    public static  double wvar( double[] values, float[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],int[]) */
    public static  double wvar( double[] values, int[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],long[]) */
    public static  double wvar( double[] values, long[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],short[]) */
    public static  double wvar( double[] values, short[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],io.deephaven.vector.DoubleVector) */
    public static  double wvar( double[] values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],io.deephaven.vector.FloatVector) */
    public static  double wvar( double[] values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],io.deephaven.vector.IntVector) */
    public static  double wvar( double[] values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],io.deephaven.vector.LongVector) */
    public static  double wvar( double[] values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(double[],io.deephaven.vector.ShortVector) */
    public static  double wvar( double[] values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],double[]) */
    public static  double wvar( float[] values, double[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],float[]) */
    public static  double wvar( float[] values, float[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],int[]) */
    public static  double wvar( float[] values, int[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],long[]) */
    public static  double wvar( float[] values, long[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],short[]) */
    public static  double wvar( float[] values, short[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],io.deephaven.vector.DoubleVector) */
    public static  double wvar( float[] values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],io.deephaven.vector.FloatVector) */
    public static  double wvar( float[] values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],io.deephaven.vector.IntVector) */
    public static  double wvar( float[] values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],io.deephaven.vector.LongVector) */
    public static  double wvar( float[] values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(float[],io.deephaven.vector.ShortVector) */
    public static  double wvar( float[] values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],double[]) */
    public static  double wvar( int[] values, double[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],float[]) */
    public static  double wvar( int[] values, float[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],int[]) */
    public static  double wvar( int[] values, int[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],long[]) */
    public static  double wvar( int[] values, long[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],short[]) */
    public static  double wvar( int[] values, short[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],io.deephaven.vector.DoubleVector) */
    public static  double wvar( int[] values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],io.deephaven.vector.FloatVector) */
    public static  double wvar( int[] values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],io.deephaven.vector.IntVector) */
    public static  double wvar( int[] values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],io.deephaven.vector.LongVector) */
    public static  double wvar( int[] values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(int[],io.deephaven.vector.ShortVector) */
    public static  double wvar( int[] values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],double[]) */
    public static  double wvar( long[] values, double[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],float[]) */
    public static  double wvar( long[] values, float[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],int[]) */
    public static  double wvar( long[] values, int[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],long[]) */
    public static  double wvar( long[] values, long[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],short[]) */
    public static  double wvar( long[] values, short[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],io.deephaven.vector.DoubleVector) */
    public static  double wvar( long[] values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],io.deephaven.vector.FloatVector) */
    public static  double wvar( long[] values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],io.deephaven.vector.IntVector) */
    public static  double wvar( long[] values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],io.deephaven.vector.LongVector) */
    public static  double wvar( long[] values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(long[],io.deephaven.vector.ShortVector) */
    public static  double wvar( long[] values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],double[]) */
    public static  double wvar( short[] values, double[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],float[]) */
    public static  double wvar( short[] values, float[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],int[]) */
    public static  double wvar( short[] values, int[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],long[]) */
    public static  double wvar( short[] values, long[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],short[]) */
    public static  double wvar( short[] values, short[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],io.deephaven.vector.DoubleVector) */
    public static  double wvar( short[] values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],io.deephaven.vector.FloatVector) */
    public static  double wvar( short[] values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],io.deephaven.vector.IntVector) */
    public static  double wvar( short[] values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],io.deephaven.vector.LongVector) */
    public static  double wvar( short[] values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(short[],io.deephaven.vector.ShortVector) */
    public static  double wvar( short[] values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,byte[]) */
    public static  double wvar( io.deephaven.vector.ByteVector values, byte[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,double[]) */
    public static  double wvar( io.deephaven.vector.ByteVector values, double[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,float[]) */
    public static  double wvar( io.deephaven.vector.ByteVector values, float[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,int[]) */
    public static  double wvar( io.deephaven.vector.ByteVector values, int[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,long[]) */
    public static  double wvar( io.deephaven.vector.ByteVector values, long[] weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,io.deephaven.vector.ByteVector) */
    public static  double wvar( io.deephaven.vector.ByteVector values, io.deephaven.vector.ByteVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,io.deephaven.vector.DoubleVector) */
    public static  double wvar( io.deephaven.vector.ByteVector values, io.deephaven.vector.DoubleVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,io.deephaven.vector.FloatVector) */
    public static  double wvar( io.deephaven.vector.ByteVector values, io.deephaven.vector.FloatVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,io.deephaven.vector.IntVector) */
    public static  double wvar( io.deephaven.vector.ByteVector values, io.deephaven.vector.IntVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ByteNumericPrimitives#wvar(io.deephaven.vector.ByteVector,io.deephaven.vector.LongVector) */
    public static  double wvar( io.deephaven.vector.ByteVector values, io.deephaven.vector.LongVector weights ) {return ByteNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,double[]) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, double[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,float[]) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, float[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,int[]) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, int[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,long[]) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, long[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,short[]) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, short[] weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,io.deephaven.vector.DoubleVector) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, io.deephaven.vector.DoubleVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,io.deephaven.vector.FloatVector) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, io.deephaven.vector.FloatVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,io.deephaven.vector.IntVector) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, io.deephaven.vector.IntVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,io.deephaven.vector.LongVector) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, io.deephaven.vector.LongVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.DoubleNumericPrimitives#wvar(io.deephaven.vector.DoubleVector,io.deephaven.vector.ShortVector) */
    public static  double wvar( io.deephaven.vector.DoubleVector values, io.deephaven.vector.ShortVector weights ) {return DoubleNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,double[]) */
    public static  double wvar( io.deephaven.vector.FloatVector values, double[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,float[]) */
    public static  double wvar( io.deephaven.vector.FloatVector values, float[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,int[]) */
    public static  double wvar( io.deephaven.vector.FloatVector values, int[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,long[]) */
    public static  double wvar( io.deephaven.vector.FloatVector values, long[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,short[]) */
    public static  double wvar( io.deephaven.vector.FloatVector values, short[] weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,io.deephaven.vector.DoubleVector) */
    public static  double wvar( io.deephaven.vector.FloatVector values, io.deephaven.vector.DoubleVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,io.deephaven.vector.FloatVector) */
    public static  double wvar( io.deephaven.vector.FloatVector values, io.deephaven.vector.FloatVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,io.deephaven.vector.IntVector) */
    public static  double wvar( io.deephaven.vector.FloatVector values, io.deephaven.vector.IntVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,io.deephaven.vector.LongVector) */
    public static  double wvar( io.deephaven.vector.FloatVector values, io.deephaven.vector.LongVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.FloatNumericPrimitives#wvar(io.deephaven.vector.FloatVector,io.deephaven.vector.ShortVector) */
    public static  double wvar( io.deephaven.vector.FloatVector values, io.deephaven.vector.ShortVector weights ) {return FloatNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,double[]) */
    public static  double wvar( io.deephaven.vector.IntVector values, double[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,float[]) */
    public static  double wvar( io.deephaven.vector.IntVector values, float[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,int[]) */
    public static  double wvar( io.deephaven.vector.IntVector values, int[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,long[]) */
    public static  double wvar( io.deephaven.vector.IntVector values, long[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,short[]) */
    public static  double wvar( io.deephaven.vector.IntVector values, short[] weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,io.deephaven.vector.DoubleVector) */
    public static  double wvar( io.deephaven.vector.IntVector values, io.deephaven.vector.DoubleVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,io.deephaven.vector.FloatVector) */
    public static  double wvar( io.deephaven.vector.IntVector values, io.deephaven.vector.FloatVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,io.deephaven.vector.IntVector) */
    public static  double wvar( io.deephaven.vector.IntVector values, io.deephaven.vector.IntVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,io.deephaven.vector.LongVector) */
    public static  double wvar( io.deephaven.vector.IntVector values, io.deephaven.vector.LongVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.IntegerNumericPrimitives#wvar(io.deephaven.vector.IntVector,io.deephaven.vector.ShortVector) */
    public static  double wvar( io.deephaven.vector.IntVector values, io.deephaven.vector.ShortVector weights ) {return IntegerNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,double[]) */
    public static  double wvar( io.deephaven.vector.LongVector values, double[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,float[]) */
    public static  double wvar( io.deephaven.vector.LongVector values, float[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,int[]) */
    public static  double wvar( io.deephaven.vector.LongVector values, int[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,long[]) */
    public static  double wvar( io.deephaven.vector.LongVector values, long[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,short[]) */
    public static  double wvar( io.deephaven.vector.LongVector values, short[] weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,io.deephaven.vector.DoubleVector) */
    public static  double wvar( io.deephaven.vector.LongVector values, io.deephaven.vector.DoubleVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,io.deephaven.vector.FloatVector) */
    public static  double wvar( io.deephaven.vector.LongVector values, io.deephaven.vector.FloatVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,io.deephaven.vector.IntVector) */
    public static  double wvar( io.deephaven.vector.LongVector values, io.deephaven.vector.IntVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,io.deephaven.vector.LongVector) */
    public static  double wvar( io.deephaven.vector.LongVector values, io.deephaven.vector.LongVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.LongNumericPrimitives#wvar(io.deephaven.vector.LongVector,io.deephaven.vector.ShortVector) */
    public static  double wvar( io.deephaven.vector.LongVector values, io.deephaven.vector.ShortVector weights ) {return LongNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,double[]) */
    public static  double wvar( io.deephaven.vector.ShortVector values, double[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,float[]) */
    public static  double wvar( io.deephaven.vector.ShortVector values, float[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,int[]) */
    public static  double wvar( io.deephaven.vector.ShortVector values, int[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,long[]) */
    public static  double wvar( io.deephaven.vector.ShortVector values, long[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,short[]) */
    public static  double wvar( io.deephaven.vector.ShortVector values, short[] weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,io.deephaven.vector.DoubleVector) */
    public static  double wvar( io.deephaven.vector.ShortVector values, io.deephaven.vector.DoubleVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,io.deephaven.vector.FloatVector) */
    public static  double wvar( io.deephaven.vector.ShortVector values, io.deephaven.vector.FloatVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,io.deephaven.vector.IntVector) */
    public static  double wvar( io.deephaven.vector.ShortVector values, io.deephaven.vector.IntVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,io.deephaven.vector.LongVector) */
    public static  double wvar( io.deephaven.vector.ShortVector values, io.deephaven.vector.LongVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
    /** @see io.deephaven.function.ShortNumericPrimitives#wvar(io.deephaven.vector.ShortVector,io.deephaven.vector.ShortVector) */
    public static  double wvar( io.deephaven.vector.ShortVector values, io.deephaven.vector.ShortVector weights ) {return ShortNumericPrimitives.wvar( values, weights );}
}

