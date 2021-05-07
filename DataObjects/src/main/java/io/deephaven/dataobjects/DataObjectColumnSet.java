/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.*;
import org.jdom2.Element;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class DataObjectColumnSet implements Serializable {

    private static final long serialVersionUID = 43523562523543L;

    protected String name_;

    private transient Element columnSetInfo_=new Element("blank");

    private DataObjectColumn[] columns_;
    private DataObjectColumn[] keys_;
    private DataObjectColumn[] indexColumns_;

    private HashMap<String, DataObjectColumn> nameToColumn_;
    private final transient Class<? extends AbstractDataObject> adoClass_;

    public DataObjectColumnSet(String name, DataObjectColumn[] columns){
        name_=name;

        columns_=columns != null ? columns : new DataObjectColumn[0];

        init();

        Class<? extends AbstractDataObject> adoClass;
        try {
            adoClass = ClassUtil.lookupClass("io.deephaven.dataobjects.generated.Default" + name).asSubclass(AbstractDataObject.class);
        }
        catch (ClassNotFoundException e) {
            adoClass = null;
        }

        adoClass_= adoClass;
    }

    public DataObjectColumnSet(String name, List<DataObjectColumn> columns){
        this(name, DataObjectColumn[].class.cast(columns.toArray(new DataObjectColumn[columns.size()])));
    }

    public DataObjectColumnSet(DataObjectColumnSet other) {
        this(other.name_, Arrays.stream(other.columns_).map(x -> x.clone()).toArray(DataObjectColumn[]::new));
    }

    private void init(){
        TreeMap<String, DataObjectColumn> keys = new TreeMap<>();

        nameToColumn_= new HashMap<>();

        for (int i=0; i<columns_.length; i++){
            columns_[i].setIndex(i);

            nameToColumn_.put(columns_[i].getName(), columns_[i]);

            if (columns_[i].isKey()){
                keys.put(columns_[i].getName(), columns_[i]);
            }
        }

        keys_= DataObjectColumn[].class.cast(keys.values().toArray(new DataObjectColumn[keys.size()]));
    }

    public void setColumnSetInfo(Element columnSetInfo){
        columnSetInfo_=columnSetInfo;
    }

    public String getName(){
        return name_;
    }

    public void setName(String name){
        name_=name;
    }

    public DataObjectColumn[] getColumns(){
        return columns_;
    }

    public DataObjectColumn[] getKeys(){
        return keys_;
    }

    public DataObjectColumn[] getIndexColumns() {
        if (indexColumns_ == null) {
            String indexedColsString = getAttributeValue("indexColumns");

            if (indexedColsString == null || indexedColsString.length()==0){
                indexColumns_ = new DataObjectColumn[0];
            }
            else{
                String indexedCols[] = indexedColsString.split(",");

                indexColumns_ = new DataObjectColumn[indexedCols.length];

                for (int i=0;i<indexColumns_.length;i++){
                    indexColumns_[i] = getExternalColumn(indexedCols[i]);
                }
            }
        }

        return indexColumns_;
    }

    public void setKeys(DataObjectColumn keys[]){
        keys_=keys;
    }

    //special case with keys == null for Aggregated ADO's with no aggregation keys
    public void setNullKeys() {
        keys_ = null;
    }

    public void addColumn(DataObjectColumn column){
        columns_=ArrayUtil.addToArray(column, columns_, DataObjectColumn.class);

        init();
    }

    public void removeColumn(DataObjectColumn column){
        //i'm not going to support removing columns cause it will just mess up all the indices..
        //instead we'll just replace it with a blank column

        for ( int i = 0; i < columns_.length; ++i ) {
            if (columns_[i].equals(column)) {
                columns_[i]=new DataObjectColumn("", "", "String");
            }
        }

        init();
    }

    public DataObjectColumn getColumn(int index){
        return columns_[index];
    }

    public DataObjectColumn getColumn(String name){
        return nameToColumn_.get(name);
    }

    public DataObjectColumn[] getColumns(String commaSeparatedColumns){
        if(commaSeparatedColumns==null || commaSeparatedColumns.isEmpty())
            return getColumns();

        String[] columnNames = commaSeparatedColumns.split(",");
        DataObjectColumn[] columns=new DataObjectColumn[columnNames.length];

        for(int i=0;i<columns.length; i++) {
            columns[i]=getColumn(columnNames[i]);
        }
        return columns;
    }

    public int getColumnIndex(String name){
        DataObjectColumn column = nameToColumn_.get(name);

        return (column==null) ? -1 : column.getIndex();
    }

    public int[] getColumnIndicesWithRefs(String[] name){
        int[] index = new int[name.length];

        DataObjectColumnSet colSet=this;

        for (int i=0; i<name.length-1; i++){
            index[i]=colSet.getColumnIndex(name[i]);

            if (index[i]==-1){
                return null;
            }

            String colSetName = colSet.getColumn(name[i]).getAttributeValue("colset");

            colSet= DataObjectColumnSetManager.getInstance().getColumnSet(colSetName);
        }

        index[name.length-1]=colSet.getColumnIndex(name[name.length-1]);

        if (index[name.length-1]==-1){
            return null;
        }

        return makeFlyweight(index);
    }

    public DataObjectColumn getExternalColumn(String name){
        String names[] = name.split("\\.");

        DataObjectColumnSet colSet=this;

        for (int i=0; i<names.length-1; i++){
            String colSetName = colSet.getColumn(names[i]).getAttributeValue("colset");

            colSet= DataObjectColumnSetManager.getInstance().getColumnSet(colSetName);
        }

        DataObjectColumn col = (DataObjectColumn)colSet.getColumn(names[names.length-1]).clone();

        col.setName(name);

        return col;
    }

    public int[] getColumnIndicesWithRefs(String name){
        int index=getColumnIndex(name);

        if (index!=-1){
            return new int[]{index};
        }
        else{
            return getColumnIndicesWithRefs(name.split("\\."));
        }
    }

    public int getSize(){
        return columns_.length;
    }

    public String getAttributeValue(String name){
        return columnSetInfo_.getAttributeValue(name);
    }

    public String getAttributeValue(String name, String defaultValue){
        return columnSetInfo_.getAttributeValue(name, defaultValue);
    }

    public HashMap<String,Class> generateAllColNames() {
        HashMap<String,Class> possibleCols = new HashMap<>();
        DataObjectColumnSet.generateAllColNames(this, "", possibleCols);
        return possibleCols;
    }
    public List<String> getAllColNames() {
        ArrayList<String> result = new ArrayList<>();
        DataObjectColumn[] cols = getColumns();
        for (DataObjectColumn col : cols) {
            result.add(col.getName());
        }
        return result;
    }

    private static HashMap generateAllColNames(DataObjectColumnSet colSet, String prefix, HashMap<String,Class> possibleCols) {
         DataObjectColumn[] cols = colSet.getColumns();
        ArrayList<String> refObjNamesList = new ArrayList<>();

        for (DataObjectColumn col : cols) {
            if (col.isBasicType()) {
                String name = prefix + col.getName();
                possibleCols.put(name, col.getType());
            } else if (col.isADO()) {
                refObjNamesList.add(col.getName());
            }
        }

        String[] refObjNames = refObjNamesList.toArray(new String[refObjNamesList.size()]);

        for (String refObjName : refObjNames) {
            String nextColSetName = colSet.getColumn(refObjName).getAttributeValue("colset");
            DataObjectColumnSet nextColSet = DataObjectColumnSetManager.getInstance().getColumnSet(nextColSetName);
            //avoid getting into an infinite recursion with self-referential refObjects
            //i.e. Security.UnderlyingSecurity.UnderlyingSecurity.UnderlyingSecurity
            if (!prefix.contains("." + refObjName)) {
                generateAllColNames(nextColSet, prefix + refObjName + ".", possibleCols);
            }
        }
        return possibleCols;
    }

    public AbstractDataObject createADOInstance() throws IllegalAccessException, InstantiationException {
        return adoClass_.newInstance();
    }

    public static String[] getNames(final DataObjectColumn[] columnArray) {
        String[] stringArray=new String[columnArray.length];
        for (int i=0; i<columnArray.length; i++) {
            stringArray[i]=columnArray[i].getName();
        }
        return stringArray;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof DataObjectColumnSet)) {
            return false;
        }
        DataObjectColumnSet otherColSet = (DataObjectColumnSet)obj;
        DataObjectColumn otherCols[] = otherColSet.getColumns();
        if (columns_ != null && otherCols != null) {
            if (columns_.length != otherCols.length) return false;
            for (int i=0;i<columns_.length;i++) {
                if (!columns_[i].equals(otherCols[i])) return false;
            }
            return true;
        }
        return columns_ == null && otherCols == null;
    }

    /**
     * Do the two DataObjectColumnSet instances contain the same WColumns?  DataObjectColumn.equal compares name and type
     * @param obj other DataObjectColumnSet
     * @return true if the DataObjectColumnSet instances contain the same WColumns in any order
     */
    public boolean equalsIgnoreOrder(Object obj) {
        if (!(obj instanceof DataObjectColumnSet)) {
            return false;
        }
        DataObjectColumn otherCols[] = ((DataObjectColumnSet)obj).getColumns();
        if (columns_.length != otherCols.length) return false;

        Set<DataObjectColumn> otherColSet = new HashSet<>(Arrays.asList(otherCols)); // Arrays.asList() is O(1)
        // HashSet.containsAll is N * O(1) contains()
        return columns_.length == otherColSet.size() && otherColSet.containsAll(Arrays.asList(columns_));
    }

    public int hashCode() {
        int hash = 37;
        if (columns_ != null) {
            int colsUsed = (columns_.length < 3)?columns_.length:3;
            for (int i=0;i<colsUsed;i++)
                hash = hash*17 + columns_[i].getName().hashCode();
        }
        return hash;
    }

    //################################################################

    private static final ConcurrentMap<int[], int[]> intArrayToIntArrayMap_=new KeyedObjectHashMap<>(new KeyedObjectKey<int[], int[]>() {
        public int[] getKey(int[] ints) { return ints; }
        public int hashKey(int[] ints) { return Arrays.hashCode(ints); }
        public boolean equalKey(int[] leftInts, int[] rightInts) { return Arrays.equals(leftInts, rightInts); }
    });

    //----------------------------------------------------------------
    /** Canonicalizes an array of ints, so that we reuse the same
     * array every time we want an array of those specific values
     * (rather than creating millions of different arrays with the
     * same values). (Flyweight pattern, GoF) */
    private static int[] makeFlyweight(int[] ints) {
        int[] canonicalInts=intArrayToIntArrayMap_.putIfAbsent(ints, ints);
        if (null==canonicalInts) {
            canonicalInts=ints;
        }
        return canonicalInts;
    }

    private void readObject(ObjectInputStream aInputStream) throws IOException, ClassNotFoundException {
        //always perform the default de-serialization first
        aInputStream.defaultReadObject();

        if (columns_ == null) {
            columns_ = new DataObjectColumn[0];
        }
    }
}
