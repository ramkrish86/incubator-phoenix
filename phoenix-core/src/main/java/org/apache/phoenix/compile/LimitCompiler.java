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
package org.apache.phoenix.compile;

import java.sql.SQLException;

import org.apache.phoenix.parse.*;
import org.apache.phoenix.schema.*;


public class LimitCompiler {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    public static final PDatum LIMIT_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }
        @Override
        public PDataType getDataType() {
            return PDataType.INTEGER;
        }
        @Override
        public Integer getByteSize() {
            return getDataType().getByteSize();
        }
        @Override
        public Integer getMaxLength() {
            return null;
        }
        @Override
        public Integer getScale() {
            return null;
        }
		@Override
		public ColumnModifier getColumnModifier() {
			return null;
		}
    };
    
    private LimitCompiler() {
    }

    public static Integer compile(StatementContext context, FilterableStatement statement) throws SQLException {
        LimitNode limitNode = statement.getLimit();
        if (limitNode == null) {
            return null;
        }
        LimitParseNodeVisitor visitor = new LimitParseNodeVisitor(context);
        limitNode.getLimitParseNode().accept(visitor);
        return visitor.getLimit();
    }
    
    private static class LimitParseNodeVisitor extends TraverseNoParseNodeVisitor<Void> {
        private final StatementContext context;
        private Integer limit;
        
        private LimitParseNodeVisitor(StatementContext context) {
            this.context = context;
        }
        
        public Integer getLimit() {
            return limit;
        }
        
        @Override
        public Void visit(LiteralParseNode node) throws SQLException {
            Object limitValue = node.getValue();
            // If limit is null, leave this.limit set to zero
            // This means that we've bound limit to null for the purpose of
            // collecting parameter metadata.
            if (limitValue != null) {
                Integer limit = (Integer)LIMIT_DATUM.getDataType().toObject(limitValue, node.getType());
                if (limit.intValue() >= 0) { // TODO: handle LIMIT 0
                    this.limit = limit;
                }
            }
            return null;
        }
    
        @Override
        public Void visit(BindParseNode node) throws SQLException {
            Object value = context.getBindManager().getBindValue(node);
            context.getBindManager().addParamMetaData(node, LIMIT_DATUM);
            // Resolve the bind value, create a LiteralParseNode, and call the visit method for it.
            // In this way, we can deal with just having a literal on one side of the expression.
            visit(NODE_FACTORY.literal(value, LIMIT_DATUM.getDataType()));
            return null;
        }
		
    }

}
