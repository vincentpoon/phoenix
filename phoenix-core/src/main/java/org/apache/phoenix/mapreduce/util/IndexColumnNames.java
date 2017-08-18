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
package org.apache.phoenix.mapreduce.util;

import java.util.Collections;
import java.util.List;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Gets index column names and their data table equivalents
 */
public class IndexColumnNames {
    private List<String> dataNonPkColNames = Lists.newArrayList();
    private List<String> dataPkColNames = Lists.newArrayList();
    private List<String> dataColNames;
    private List<String> dataColSqlTypeNames;
    private List<String> indexPkColNames = Lists.newArrayList();
    private List<String> indexNonPkColNames = Lists.newArrayList();
    private List<String> indexColNames;
    private List<String> indexColSqlTypeNames;
    private PTable pdataTable;
    private PTable pindexTable;

    public IndexColumnNames(PTable pdataTable, PTable pindexTable) {
        this.pdataTable = pdataTable;
        this.pindexTable = pindexTable;
        int dataPKColIndex = 0;
        List<PColumn> pindexCols = pindexTable.getColumns();
        List<String> dataNonPkSqlTypeNames = Lists.newArrayList();
        List<String> dataPkSqlTypeNames = Lists.newArrayList();
        for (PColumn pIndexCol : pindexCols) {
            String indexColName = pIndexCol.getName().getString();
            if (SchemaUtil.isPKColumn(pIndexCol)) {
                indexPkColNames.add(indexColName);
            } else {
                indexNonPkColNames.add(indexColName);
            }

            PColumn dCol = IndexUtil.getDataColumn(pdataTable, indexColName);
            String dataColFullName = getDataColFullName(dCol);
            if (IndexUtil.isDataPKColumn(pIndexCol)) {
                // in our list of indexPkCols, put the data pk cols first so that we can easily grab
                // them from the resultset
                // to form our scrutiny query against the data table
                Collections.swap(indexPkColNames, dataPKColIndex++, indexPkColNames.size() - 1);
                dataPkColNames.add(dataColFullName);
                dataPkSqlTypeNames.add(dCol.getDataType().toString());
            } else {
                dataNonPkColNames.add(dataColFullName);
                dataNonPkSqlTypeNames.add(dCol.getDataType().toString());
            }
        }

        dataColNames = Lists.newArrayList(Iterables.concat(dataPkColNames, dataNonPkColNames));
        indexColNames = Lists.newArrayList(Iterables.concat(indexPkColNames, indexNonPkColNames));
        dataColSqlTypeNames =
                Lists.newArrayList(Iterables.concat(dataPkSqlTypeNames, dataNonPkSqlTypeNames));
        indexColSqlTypeNames = getSqlTypeNames(pindexCols, indexColNames);
    }

    /**
     * @return the data types for pCols, ordered by orderColNames
     */
    private List<String> getSqlTypeNames(List<PColumn> pCols, List<String> orderColNames) {
        String[] sqlTypeArr = new String[pCols.size()];
        for (int i = 0; i < pCols.size(); i++) {
            PColumn pCol = pCols.get(i);
            String colName = pCol.getName().getString();
            int colNamesIdx = orderColNames.indexOf(colName);
            sqlTypeArr[colNamesIdx] = pCol.getDataType().toString();
        }
        return Lists.newArrayList(sqlTypeArr);
    }

    private String getDataColFullName(PColumn dCol) {
        String dColFullName = "";
        if (dCol.getFamilyName() != null) {
            dColFullName += dCol.getFamilyName().getString() + QueryConstants.NAME_SEPARATOR;
        }
        dColFullName += dCol.getName().getString();
        return dColFullName;
    }

    private List<String> getDynamicCols(List<String> colNames, List<String> colTypes) {
        List<String> dynamicCols = Lists.newArrayListWithCapacity(colNames.size());
        for (int i = 0; i < colNames.size(); i++) {
            String dataColName = colNames.get(i);
            String dataColType = colTypes.get(i);
            String dynamicCol =
                    SchemaUtil.getEscapedFullColumnName(dataColName) + " " + dataColType;
            dynamicCols.add(dynamicCol);
        }
        return dynamicCols;
    }

    private List<String> getUnqualifiedColNames(List<String> qualifiedCols) {
        return Lists.transform(qualifiedCols, new Function<String, String>() {
            @Override
            public String apply(String qCol) {
                return SchemaUtil.getTableNameFromFullName(qCol, QueryConstants.NAME_SEPARATOR);
            }
        });
    }

    public String getQualifiedDataTableName() {
        return SchemaUtil.getQualifiedTableName(pdataTable.getSchemaName().getString(),
            pdataTable.getTableName().getString());
    }

    public String getQualifiedIndexTableName() {
        return SchemaUtil.getQualifiedTableName(pindexTable.getSchemaName().getString(),
            pindexTable.getTableName().getString());
    }

    /**
     * @return the escaped data column names (equivalents for the index columns) along with their
     *         sql type, for use in dynamic column queries/upserts
     */
    public List<String> getDynamicDataCols() {
        // don't want the column family for dynamic columns
        return getDynamicCols(getUnqualifiedDataColNames(), dataColSqlTypeNames);

    }

    /**
     * @return the escaped index column names along with their sql type, for use in dynamic column
     *         queries/upserts
     */
    public List<String> getDynamicIndexCols() {
        // don't want the column family for dynamic columns
        return getDynamicCols(getUnqualifiedIndexColNames(), indexColSqlTypeNames);
    }

    /**
     * @return the corresponding data table column names for the index columns, leading with the
     *         data table pk columns
     */
    public List<String> getDataColNames() {
        return dataColNames;
    }

    /**
     * @return same as getDataColNames, without the column family qualifier
     */
    public List<String> getUnqualifiedDataColNames() {
        return getUnqualifiedColNames(dataColNames);
    }

    /**
     * @return the corresponding data table column names for the index columns, which are not part
     *         of the data table pk
     */
    public List<String> getDataNonPkColNames() {
        return dataNonPkColNames;
    }

    /**
     * @return the corresponding data table column names for the index columns, which are part of
     *         the data table pk
     */
    public List<String> getDataPkColNames() {
        return dataPkColNames;
    }

    /**
     * @return the index column names, leading with the data table pk columns
     */
    public List<String> getIndexColNames() {
        return indexColNames;
    }

    /**
     * @return same as getIndexColNames, without the column family qualifier
     */
    public List<String> getUnqualifiedIndexColNames() {
        return getUnqualifiedColNames(indexColNames);
    }

    /**
     * @return the index pk column names
     */
    public List<String> getIndexPkColNames() {
        return indexPkColNames;
    }
}
