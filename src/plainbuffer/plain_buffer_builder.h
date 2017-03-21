/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef PLAIN_BUFFER_BUILDER_H
#define PLAIN_BUFFER_BUILDER_H

#include "ots/ots_request.h"
#include "plain_buffer_coded_stream.h"
#include <string>

namespace aliyun {
namespace tablestore {

class PlainBufferBuilder
{
public:

    static int32_t ComputePrimaryKeyValueSize(const PrimaryKeyValue& value);
    
    static int32_t ComputeVariantValueSize(const PrimaryKeyValue& value);

    static int32_t ComputePrimaryKeyColumnSize(const PrimaryKeyColumn& pkColumn);
    
    static int32_t ComputeColumnValueSize(const ColumnValue& value);

    static int32_t ComputeVariantValueSize(const ColumnValue& value);
    
    static int32_t ComputeColumnSize(const Column& column);
    
    static int32_t ComputeColumnSize(const Column& column, RowUpdateChange::UpdateType type);
    
    static int32_t ComputePrimaryKeySize(const PrimaryKey& primaryKey);
    
    static int32_t ComputeRowSize(const RowPutChange& rowChange);
    
    static int32_t ComputeRowSize(const RowUpdateChange& rowChange);
    
    static int32_t ComputeRowSize(const RowDeleteChange& rowChange);

    static std::string SerializePrimaryKeyValue(const PrimaryKeyValue& value);

    static std::string SerializeColumnValue(const ColumnValue& value);

    static std::string SerializePrimaryKey(const PrimaryKey& primaryKey);

    static std::string SerializeForRow(const RowPutChange& rowChange); 

    static std::string SerializeForRow(const RowUpdateChange& rowChange); 

    static std::string SerializeForRow(const RowDeleteChange& rowChange); 
};

} // end of tablestore
} // end of aliyun

#endif
