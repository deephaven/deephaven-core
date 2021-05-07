/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.dataobjects;

import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import org.jdom2.*;
import org.jdom2.input.SAXBuilder;

import java.io.InputStream;
import java.util.*;

public class DataObjectColumnSetManager {
    private static final String defaultColumnClass = Configuration.getInstance().getStringWithDefault("columnsetmanager.defaultColumnClass", "io.deephaven.gui.table.WAbstractDataColumn");

    private static volatile DataObjectColumnSetManager instance_=null;

    private boolean isServerMode_=true;

    private HashMap<String, DataObjectColumnSet> columnSets_= new HashMap<>();
    private HashMap<String,String> columnSetRenames = new HashMap<>();

    private static final io.deephaven.io.logger.Logger log = LoggerFactory.getLogger(DataObjectColumnSetManager.class);

    private DataObjectColumnSetManager(String singleSourceName){
        isServerMode_=System.getProperty("isServerMode", "true").equalsIgnoreCase("true");

        loadColumnSets(singleSourceName);
    }


    public static DataObjectColumnSetManager getInstance(){
        return getInstance(null);
    }

    public static DataObjectColumnSetManager getInstance(String singleSourceName){
        if (instance_ == null) {
            synchronized (DataObjectColumnSetManager.class) {
                if (instance_ == null) {
                    instance_ = new DataObjectColumnSetManager(singleSourceName);
                }
            }
        }

        return instance_;
    }

    public static boolean isValidColumnType(String type) {
        if (type == null) {
            return false;
        }
        if (type.equals("Date")) {
            return true;
        } else {
            try {
                Class.forName("java.lang." + type);

                return true;
            }
            catch (ClassNotFoundException e) {
                return false;
            }
        }
    }

    private void addColumnInfos(HashMap<String, ArrayList<Element>> columnInfos, String fileName) throws Exception {
        final InputStream resourceAsStream = getClass().getResourceAsStream("/" + fileName);
        if (resourceAsStream == null) {
            throw new RuntimeException("Could not load columns: " + fileName);
        }
        Element root=new SAXBuilder().build(resourceAsStream).getRootElement();

        String extend=root.getAttributeValue("extends");

        if (extend!=null){
            addColumnInfos(columnInfos, extend);
        }

        for (Element columnSetInfo : root.getChildren("ColumnSet")) {
            final String name = columnSetInfo.getAttributeValue("name");

            ArrayList<Element> columnInfosForName = columnInfos.computeIfAbsent(name, k -> new ArrayList<>());
            columnInfosForName.add(columnSetInfo);

            final String oldName = columnSetInfo.getAttributeValue("oldName");
            if (oldName != null) {
                columnSetRenames.put(oldName, name);
            }
        }
    }

    private void loadColumnSets(String singleSourceName){
        try {
            LinkedHashMap<String, ArrayList<Element>> columnInfos = new LinkedHashMap<>();

            final String columnsFiles = Configuration.getInstance().getStringWithDefault("columnsFile", "Columns.xml;DeephavenColumns.xml");
            for (String columnsFile : columnsFiles.split(";")) {
                addColumnInfos(columnInfos, columnsFile);
            }

            for (Map.Entry<String, ArrayList<Element>> entry : columnInfos.entrySet()){
                String name=entry.getKey();
                DataObjectColumnSet columnSet = createColumnSet(name, entry.getValue(), false);
                if(singleSourceName!=null && !singleSourceName.equals(name))
                    continue;
                addColumnSet(columnSet);

                //--------------------------------------------------------------------------------------------

                if (!isServerMode_){
                    addColumnSet(createColumnSet(name + "-VIEW", entry.getValue(), true));
                }

                //--------------------------------------------------------------------------------------------

                String aliasList=entry.getValue().get(0).getAttributeValue("alias");   //just use the root one for this...

                if (aliasList!=null){
                    for (String alias : aliasList.split(",")) {
                        addColumnSet(alias.trim(), columnSet);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Could not load column sets!", e);
        }
    }

    private DataObjectColumnSet createColumnSet(String columnSetName, List<Element> columnSetInfos, boolean isView) throws Exception {
        LinkedHashMap<String, DataObjectColumn> columnList = new LinkedHashMap<>();

        for (Element columnSetInfo : columnSetInfos){
            for (Element columnInfo : columnSetInfo.getChildren()) {
                if (columnInfo.getName().equals("Column")) {
                    DataObjectColumn column;

                    if (isServerMode_) {
                        column = new DataObjectColumn(columnInfo);
                    } else {
                        if (isView && !isValidColumnType(columnInfo.getAttributeValue("type")) && !"true".equals(columnInfo.getAttributeValue("shouldBeDisplayed"))) {
                            continue;
                        }

                        final String columnClass = columnInfo.getAttributeValue("class", defaultColumnClass);

                        try {
                            column = (DataObjectColumn) Class.forName(columnClass).getConstructor(new Class[]{Element.class}).newInstance(columnInfo);
                        } catch (Exception e) {
                            log.error().append("Could not create colset=" + columnSetName + ".  Skipping. ").append(e).endl();

                            return null;
                        }
                    }

                    columnList.put(column.getName(), column);
                } else if (isView && columnInfo.getName().equals("CopyColumn")) {
                    String name = columnInfo.getAttributeValue("name");
                    String abbreviation = columnInfo.getAttributeValue("abbreviation");
                    String path[] = columnInfo.getAttributeValue("path").split("\\.");

                    DataObjectColumnSet columnSet = getColumnSet(path[0]);
                    if (null == columnSet) {
                        log.error("Found reference to unknown columnSet \"" + path[0] + "\" from columnSet \"" + columnSetName + "\". Skipping \"" + columnSetName + "\".");
                        return null;
                    }
                    DataObjectColumn column = columnSet.getColumn(path[1]);
                    if (null == column) {
                        log.error("Found reference to unknown column \"" + path[0] + "." + path[1] + "\" from columnSet \"" + columnSetName + "\". Skipping \"" + columnSetName + "\".");
                        return null;
                    }

                    Element clonedColumnInfo = column.getColumnInfo().clone();

                    String renderer = columnInfo.getAttributeValue("renderer");

                    if (renderer != null) {
                        clonedColumnInfo.setAttribute("renderer", renderer);
                    }

                    column = column.getClass().getConstructor(new Class[]{Element.class}).newInstance(clonedColumnInfo);

                    if (name != null) {
                        column.setName(name);
                    }

                    if (abbreviation != null) {
                        column.setAbbreviation(abbreviation);
                        column.setLabel(abbreviation);
                    }

                    for (Attribute attr : columnInfo.getAttributes()) {
                        String attrName = attr.getName();

                        if (!attrName.equals("name") && !attrName.equals("abbreviation") && !attrName.equals("path")) {
                            column.getColumnInfo().setAttribute(attrName, attr.getValue());
                        }
                    }

                    columnList.put(column.getName(), column);
                } else if (isView && columnInfo.getName().equals("ViewColumn")) {
                    if (isValidColumnType(columnInfo.getAttributeValue("type")) || "true".equals(columnInfo.getAttributeValue("shouldBeDisplayed"))) {
                        String columnClass = columnInfo.getAttributeValue("class");

                        DataObjectColumn column;

                        try {
                            column = (DataObjectColumn) Class.forName(columnClass).getConstructor(new Class[]{Element.class}).newInstance(columnInfo);
                        } catch (Exception e) {
                            log.error("Could not create colset=" + columnSetName + ".  Skipping. " + e);

                            return null;
                        }

                        columnList.put(column.getName(), column);
                    }
                }
            }
        }

        DataObjectColumnSet columnSet = new DataObjectColumnSet(columnSetName, columnList.values().toArray(new DataObjectColumn[0]));
        columnSet.setColumnSetInfo(columnSetInfos.get(0));         //use the root one for this...

        return columnSet;
    }

    public void addColumnSet(DataObjectColumnSet columnSet){
        if (columnSet!=null){
            addColumnSet(columnSet.getName(), columnSet);
        }
    }

    private void addColumnSet(String name, DataObjectColumnSet columnSet){
        if (columnSet!=null){
            columnSets_.put(name, columnSet);
        }
    }

    // If there has been a columnset rename, we need to return the new columnset if the old one is asked for
    public DataObjectColumnSet getColumnSet(String columnSetName){
        final DataObjectColumnSet columnSet = columnSets_.get(columnSetName);
        return columnSet != null ? columnSet : columnSets_.get(columnSetRenames.get(columnSetName));
    }

    public String getRenamedColumnSetName(final String columnSetName) {
        return columnSetRenames.getOrDefault(columnSetName, columnSetName);
    }

    public DataObjectColumnSet[] getColumnSets(){
        return columnSets_.values().toArray(new DataObjectColumnSet[columnSets_.size()]);
    }
}
