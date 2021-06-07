/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects;

import io.deephaven.base.ClassUtil;
import io.deephaven.datastructures.util.CollectionUtil;
import org.jdom2.Element;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class DataObjectColumn<T> implements Serializable, Comparable {

    private static final long serialVersionUID = 3284834378543L;

    protected String name_, abbreviation_, label_;
    protected Class<T> type_;

    protected boolean shouldBeSerialized_;
    protected boolean serializeAsObject_;

    protected boolean isKey_;

    protected transient String enums_[];

    protected transient Element columnInfo_;

    protected int index_;
    private Class<T> javaType_;
    private String codecName;


    public DataObjectColumn(){
        //if you use this constructor make sure you call setColumnInfo!
    }

    public DataObjectColumn(Element columnInfo){
        setColumnInfo(columnInfo);
    }

    public DataObjectColumn(DataObjectColumn other) {
        this(other.columnInfo_);
        index_ = other.index_;
        enums_ = other.enums_;
    }

    public DataObjectColumn(String name, String abbreviation, String type){
        Element columnInfo = new Element("colinfo");

        columnInfo.setAttribute("name", name);
        columnInfo.setAttribute("abbreviation", abbreviation);
        columnInfo.setAttribute("type", type);

        setColumnInfo(columnInfo);
    }

    public void setColumnInfo(Element columnInfo){
        columnInfo_=columnInfo;

        name_=columnInfo.getAttributeValue("name");

        abbreviation_=columnInfo.getAttributeValue("abbreviation");

        if (abbreviation_==null){
            abbreviation_=name_;
        }

        label_=columnInfo.getAttributeValue("label");

        if (label_==null){
            label_=abbreviation_==null ? name_ : abbreviation_;
        }

        try {
            String type = columnInfo.getAttributeValue("type");

            String typeStr = type;
            if (type.equals("Date")){
                typeStr = "java.util.Date";
            }
            else if (type.indexOf('.')==-1){
                typeStr ="java.lang."+type;
            }
            //noinspection unchecked
                type_ = (Class<T>) ClassUtil.lookupClass(typeStr);
            if (TYPE_TO_JAVA_TYPE_MAP.containsKey(type_)) {
                //noinspection unchecked
                javaType_ = TYPE_TO_JAVA_TYPE_MAP.get(type_);
            } else {
                javaType_ = type_;
            }
        }
        catch (ClassNotFoundException e) {
             throw new RuntimeException("Could not find Column class", e);
        }

        String enums=columnInfo.getAttributeValue("enum");

        if (enums!=null){
            enums_=enums.split(",");

            for (int i=0; i<enums_.length; i++){  //trim em..
                enums_[i]=enums_[i].trim();
            }
        }

        isKey_="true".equalsIgnoreCase(columnInfo.getAttributeValue("key"));

        shouldBeSerialized_= (isBasicType() || "true".equalsIgnoreCase(columnInfo.getAttributeValue("shouldBeSerialized")))
                && ! "false".equalsIgnoreCase(columnInfo.getAttributeValue("shouldBeSerialized"));
        serializeAsObject_ = "true".equalsIgnoreCase(columnInfo.getAttributeValue("serializeAsObject", "false"));
    }

    public void setName(String name){  //warning!  only call this before you insert it into a columnset!
        name_=name;
    }

    public String getName(){
        return name_;
    }

    public String getAbbreviation(){
        return abbreviation_;
    }

    public void setAbbreviation(String abbreviation){
        abbreviation_=abbreviation;
    }

    public String getLabel(){
        return label_;
    }

    public void setLabel(String label){
        label_=label;
    }

    public Class getJavaType(){
        return javaType_;
    }

    public Class getType(){
        return type_;
    }

    private static final Map<Class, Class> TYPE_TO_JAVA_TYPE_MAP= CollectionUtil.unmodifiableMapFromArray(Class.class,Class.class,
                                                                                                           Integer.class, int.class,
                                                                                                           Double.class, double.class,
                                                                                                           Long.class, long.class,
                                                                                                           Byte.class, byte.class,
                                                                                                           Date.class, long.class,
                                                                                                           Float.class, float.class);


    public void setType(Class type){
        type_=type;
        if (TYPE_TO_JAVA_TYPE_MAP.containsKey(type)) {
            javaType_ = TYPE_TO_JAVA_TYPE_MAP.get(type);
        } else {
            javaType_ = type_;
        }
    }

    public String getStrType() {
        if (type_==String.class) {
            return "String";
        }
        else if (type_==Integer.class) {
            return "Integer";
        }
        else if (type_==Double.class) {
            return "Double";
        }
        else if (type_==Long.class) {
            return "Long";
        }
        else if (type_==Byte.class) {
            return "Byte";
        }
        else if (type_==Boolean.class) {
            return "Boolean";
        }
        else if (type_==Date.class) {
            return "Date";
        }
        else if (type_==Float.class) {
            return "Float";
        } else throw new RuntimeException ("Invalid internal class.");
    }

    public boolean isBasicType() {
        if (type_==String.class
                || type_==Integer.class
                || type_==Double.class
                || type_==Long.class
                || type_==Byte.class
                || type_==Boolean.class
                || type_==Date.class
                || type_==Float.class
        ) return true;
        return false;
    }

    public boolean isADO() {
        return AbstractDataObject.class.isAssignableFrom(type_);
    }

    public String getAttributeValue(String name){
        return columnInfo_.getAttributeValue(name);
    }

    public String getAttributeValue(String name, String defaultValue){
        return columnInfo_.getAttributeValue(name, defaultValue);
    }

    public Element getColumnInfo(){
        return columnInfo_;
    }

    public void setIndex(int index){
        index_=index;
    }

    public int getIndex(){
        return index_;
    }

    public boolean isKey(){
        return isKey_;
    }

    public boolean shouldBeSerialized(){
        return shouldBeSerialized_;
    }

    public boolean serializeAsObject(){
        return serializeAsObject_;
    }

    public String[] getEnums(){
        return enums_;
    }

    public DataObjectColumn clone() {
        DataObjectColumn cloned = new DataObjectColumn(columnInfo_);
        cloned.index_ = this.index_;
        cloned.enums_ = this.enums_;
        return cloned;
    }

    public int compareTo(@NotNull Object o) {
        return toString().compareTo(o.toString());
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof DataObjectColumn)) return false;
        DataObjectColumn col = (DataObjectColumn)obj;
        return col.getName().equals(name_) && col.getType().equals(type_);
    }

    public int hashCode() {
        return name_.hashCode() ^ type_.hashCode();
    }

    public String getCodecName() {
        if (codecName == null && columnInfo_ != null) {
            codecName = columnInfo_.getAttributeValue("codec");
        }
        return codecName;
    }
}

