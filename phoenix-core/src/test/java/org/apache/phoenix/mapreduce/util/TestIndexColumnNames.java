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

import static org.junit.Assert.assertEquals;

import org.apache.phoenix.mapreduce.index.BaseIndexTest;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class TestIndexColumnNames extends BaseIndexTest {

    @Test
    public void testGetColumnNames() {
        IndexColumnNames indexColumnNames = new IndexColumnNames(pDataTable, pIndexTable);
        assertEquals("[ID, PK_PART2, 0.NAME, 0.ZIP]", indexColumnNames.getDataColNames().toString());
        assertEquals("[:ID, :PK_PART2, 0:NAME, 0:ZIP]", indexColumnNames.getIndexColNames().toString()); //index column names, leading with the data table pk
        assertEquals("[:ID, :PK_PART2, 0:NAME]", indexColumnNames.getIndexPkColNames().toString());
        assertEquals("[ID, PK_PART2]", indexColumnNames.getDataPkColNames().toString());
        assertEquals("[0.NAME, 0.ZIP]", indexColumnNames.getDataNonPkColNames().toString());

        assertEquals("[\"ID\" INTEGER, \"PK_PART2\" TINYINT, \"NAME\" VARCHAR, \"ZIP\" BIGINT]", indexColumnNames.getDynamicDataCols().toString());
        assertEquals("[\":ID\" INTEGER, \":PK_PART2\" TINYINT, \"0:NAME\" VARCHAR, \"0:ZIP\" BIGINT]", indexColumnNames.getDynamicIndexCols().toString());
        assertEquals("UPSERT /*+ NO_INDEX */  INTO TEST_SCHEMA.TEST_INDEX_COLUMN_NAMES_UTIL (\"ID\" INTEGER, \"PK_PART2\" TINYINT, \"NAME\" VARCHAR, \"ZIP\" BIGINT) VALUES (?, ?, ?, ?)", QueryUtil.constructUpsertStatement(DATA_TABLE_FULL_NAME, indexColumnNames.getDynamicDataCols(), Hint.NO_INDEX));
    }
}
