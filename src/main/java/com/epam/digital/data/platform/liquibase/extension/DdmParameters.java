/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.liquibase.extension;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class DdmParameters {
    private String historyTableSuffix;
    private String subjectTable;
    private String subjectColumn;
    private String subjectColumnType;
    private final List<DdmHistoryTableColumn> historyTableColumns;
    private final List<DdmHistoryTableColumn> dcmColumns;

    public enum Scope {ALL, PRIMARY, HISTORY}

    public DdmParameters () {
        historyTableColumns = new ArrayList<>();
        dcmColumns = new ArrayList<>();

        setHistoryTableSuffix();
        setHistoryTableColumns();
        setSubjects();
        setDcmColumns();
    }

    public static boolean isAll(String scope) {
        return Scope.ALL.name().equalsIgnoreCase(scope);
    }

    public static boolean isPrimary(String scope) {
        return Scope.PRIMARY.name().equalsIgnoreCase(scope);
    }

    public static boolean isHistory(String scope) {
        return Scope.HISTORY.name().equalsIgnoreCase(scope);
    }

    protected NodeList getHistoryFlagNodes() throws ParserConfigurationException, IOException, SAXException {
        InputStream inputFile = this.getClass().getClassLoader().getResourceAsStream(DdmConstants.PARAMETERS_FILE_NAME);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        dbFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(inputFile);

        return doc.getElementsByTagName(DdmConstants.XML_TAG_HISTORY_FLAG);
    }

    protected void setHistoryTableSuffix() {
        try {
            NodeList historyFlag = getHistoryFlagNodes();
            NodeList tableSuffix = ((Element) historyFlag.item(0)).getElementsByTagName(DdmConstants.XML_TAG_TABLE_SUFFIX);
            if (tableSuffix.item(0).getNodeType() == Node.ELEMENT_NODE) {
                this.historyTableSuffix = ((Element) tableSuffix.item(0)).getAttribute(DdmConstants.ATTRIBUTE_NAME);
            }
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new IllegalStateException(e);
        }
    }

    public String getHistoryTableSuffix() {
        return historyTableSuffix;
    }

    protected void setHistoryTableColumns() {
        try {
            NodeList historyFlag = getHistoryFlagNodes();
            NodeList allColumns = ((Element) historyFlag.item(0)).getElementsByTagName(DdmConstants.XML_TAG_COLUMNS);
            NodeList columns = ((Element) allColumns.item(0)).getElementsByTagName(DdmConstants.XML_TAG_COLUMN);
            for (int i = 0; i < columns.getLength(); i++) {
                if (columns.item(i).getNodeType() == Node.ELEMENT_NODE) {
                    Element column = (Element) columns.item(i);

                    DdmHistoryTableColumn historyTableColumn = new DdmHistoryTableColumn();
                    historyTableColumn.setName(column.getAttribute(DdmConstants.ATTRIBUTE_NAME));
                    historyTableColumn.setType(column.getAttribute(DdmConstants.ATTRIBUTE_TYPE));
                    historyTableColumn.setScope(column.getAttribute(DdmConstants.ATTRIBUTE_SCOPE));
                    historyTableColumn.setUniqueWithPrimaryKey(column.getAttribute(DdmConstants.ATTRIBUTE_UNIQUE_WITH_PRIMARY_KEY).equals(DdmConstants.ATTRIBUTE_TRUE));
                    historyTableColumn.setNullable(column.getAttribute(DdmConstants.ATTRIBUTE_NULLABLE).equals(DdmConstants.ATTRIBUTE_TRUE));
                    historyTableColumn.setDefaultValueComputed(column.getAttribute(DdmConstants.ATTRIBUTE_DEFAULT_VALUE_COMPUTED));

                    historyTableColumns.add(historyTableColumn);
                }
            }
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new IllegalStateException(e);
        }
    }

    public List<DdmHistoryTableColumn> getHistoryTableColumns() {
        return historyTableColumns;
    }

    public void setSubjects() {
        try {
            NodeList historyFlag = getHistoryFlagNodes();
            NodeList subjectTableNode = ((Element) historyFlag.item(0)).getElementsByTagName(DdmConstants.XML_TAG_SUBJECT_TABLE);
            if (subjectTableNode.item(0).getNodeType() == Node.ELEMENT_NODE) {
                setSubjectTable(((Element) subjectTableNode.item(0)).getAttribute(DdmConstants.ATTRIBUTE_NAME));
                setSubjectColumn(((Element) subjectTableNode.item(0)).getAttribute(DdmConstants.ATTRIBUTE_COLUMN));
                setSubjectColumnType(((Element) subjectTableNode.item(0)).getAttribute(DdmConstants.ATTRIBUTE_TYPE));
            }
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new IllegalStateException(e);
        }
    }

    public String getSubjectTable() {
        return subjectTable;
    }

    public void setSubjectTable(String subjectTable) {
        this.subjectTable = subjectTable;
    }

    public String getSubjectColumn() {
        return subjectColumn;
    }

    public void setSubjectColumn(String subjectColumn) {
        this.subjectColumn = subjectColumn;
    }

    public String getSubjectColumnType() {
        return subjectColumnType;
    }

    public void setSubjectColumnType(String subjectColumnType) {
        this.subjectColumnType = subjectColumnType;
    }

    public List<DdmHistoryTableColumn> getDcmColumns() {
        return dcmColumns;
    }

    public void setDcmColumns() {
        try {
            NodeList historyFlag = getHistoryFlagNodes();
            NodeList allColumns = ((Element) historyFlag.item(0)).getElementsByTagName(DdmConstants.XML_TAG_DCM_COLUMNS);
            NodeList columns = ((Element) allColumns.item(0)).getElementsByTagName(DdmConstants.XML_TAG_COLUMN);
            for (int i = 0; i < columns.getLength(); i++) {
                if (columns.item(i).getNodeType() == Node.ELEMENT_NODE) {
                    Element column = (Element) columns.item(i);

                    DdmHistoryTableColumn dcmColumn = new DdmHistoryTableColumn();
                    dcmColumn.setName(column.getAttribute(DdmConstants.ATTRIBUTE_NAME));
                    dcmColumn.setType(column.getAttribute(DdmConstants.ATTRIBUTE_TYPE));

                    dcmColumns.add(dcmColumn);
                }
            }
        } catch (ParserConfigurationException | IOException | SAXException e) {
            throw new IllegalStateException(e);
        }
    }
}
