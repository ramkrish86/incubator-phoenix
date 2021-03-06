/*
 * Copyright 2014 The Apache Software Foundation
 *
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
package org.apache.phoenix.parse;

import org.apache.phoenix.schema.PTableType;

public class DropTableStatement implements BindableStatement {
    private final TableName tableName;
    private final boolean ifExists;
    private final PTableType tableType;

    protected DropTableStatement(TableName tableName, PTableType tableType, boolean ifExists) {
        this.tableName = tableName;
        this.tableType = tableType;
        this.ifExists = ifExists;
    }
    
    @Override
    public int getBindCount() {
        return 0; // No binds for DROP
    }

    public TableName getTableName() {
        return tableName;
    }

    public PTableType getTableType() {
        return tableType;
    }

    public boolean ifExists() {
        return ifExists;
    }
}
