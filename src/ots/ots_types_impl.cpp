#include "ots_exception_impl.h"
#include "ots_factory_impl.h"
#include "ots_types_impl.h"

using namespace std;
using namespace com::aliyun;
using namespace google::protobuf;

namespace aliyun {
namespace openservices {
namespace ots {

//ErrorImpl
ErrorImpl::ErrorImpl(
            cloudservice::ots2::Error* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

ErrorImpl::~ErrorImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

const string& ErrorImpl::GetCode() const
{
    return mPBMessage->code();
}

const string& ErrorImpl::GetMessage() const
{
    return mPBMessage->message();
}

Message* ErrorImpl::GetPBMessage() const
{
    return mPBMessage;
}


// CapacityUnitImpl
CapacityUnitImpl::CapacityUnitImpl(
            cloudservice::ots2::CapacityUnit* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

CapacityUnitImpl::~CapacityUnitImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

int32_t CapacityUnitImpl::GetRead() const
{
    return (mPBMessage->has_read()) ? mPBMessage->read() : 0;
}

int32_t CapacityUnitImpl::GetWrite() const
{
    return (mPBMessage->has_write()) ? mPBMessage->write() : 0;
}

void CapacityUnitImpl::SetRead(int32_t read)
{
    mPBMessage->set_read(read);
}

void CapacityUnitImpl::SetWrite(int32_t write)
{
    mPBMessage->set_write(write);
}

Message* CapacityUnitImpl::GetPBMessage() const
{
    return mPBMessage;
}

// ReservedThroughputDetailsImpl
ReservedThroughputDetailsImpl::ReservedThroughputDetailsImpl(
            cloudservice::ots2::ReservedThroughputDetails* message,
            Ownership ownership)
: mOwnership(ownership), mCapacityUnit(NULL), mPBMessage(message)
{
}

ReservedThroughputDetailsImpl::~ReservedThroughputDetailsImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mCapacityUnit;
}

const CapacityUnit& ReservedThroughputDetailsImpl::GetCapacityUnit() const
{
    if (NULL == mCapacityUnit) {
        OTS_THROW("OTClient", "ClientInternalError", "CapacityUnit is null");
    }
    return *(mCapacityUnit);
}

int64_t ReservedThroughputDetailsImpl::GetLastIncreaseTime() const
{
    return mPBMessage->last_increase_time();
}

int64_t ReservedThroughputDetailsImpl::GetLastDecreaseTime() const
{
    return mPBMessage->last_decrease_time();
}

int32_t ReservedThroughputDetailsImpl::GetNumberOfDecreasesToday() const
{
    return mPBMessage->number_of_decreases_today();
}

google::protobuf::Message* ReservedThroughputDetailsImpl::GetPBMessage() const
{
    return mPBMessage;
}

void ReservedThroughputDetailsImpl::ParseFromPBMessage()
{
    cloudservice::ots2::CapacityUnit* pbCapacity = mPBMessage->mutable_capacity_unit();
    mCapacityUnit = OTSFactoryImpl::NewCapacityUnit(pbCapacity);
}

// ReservedThroughputImpl
ReservedThroughputImpl::ReservedThroughputImpl(
            com::aliyun::cloudservice::ots2::ReservedThroughput* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

ReservedThroughputImpl::~ReservedThroughputImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void ReservedThroughputImpl::SetCapacityUnit(CapacityUnit* capacity) const
{
    if (NULL == capacity) {
        OTS_THROW("OTSClient", "ClientParameterError", "CapacityUnit is null");
    }
    cloudservice::ots2::CapacityUnit* pbCapacity = mPBMessage->mutable_capacity_unit();
    CapacityUnitImpl* capacityImpl = dynamic_cast<CapacityUnitImpl*>(capacity);
    pbCapacity->Swap(dynamic_cast<cloudservice::ots2::CapacityUnit*>(capacityImpl->GetPBMessage()));
    delete capacity;
}

google::protobuf::Message* ReservedThroughputImpl::GetPBMessage() const
{
    return mPBMessage;
}

// OTSInfMinValueImpl
OTSInfMinValueImpl::OTSInfMinValueImpl(
            cloudservice::ots2::ColumnValue* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
    mPBMessage->set_type(cloudservice::ots2::INF_MIN);
}

OTSInfMinValueImpl::~OTSInfMinValueImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

ColumnType OTSInfMinValueImpl::GetType() const
{
    return INF_MIN;
}

Message* OTSInfMinValueImpl::GetPBMessage() const
{
    return mPBMessage;
}


// OTSInfMaxValueImpl
OTSInfMaxValueImpl::OTSInfMaxValueImpl(
            cloudservice::ots2::ColumnValue* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
    mPBMessage->set_type(cloudservice::ots2::INF_MAX);
}

OTSInfMaxValueImpl::~OTSInfMaxValueImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

ColumnType OTSInfMaxValueImpl::GetType() const
{
    return INF_MAX;
}

Message* OTSInfMaxValueImpl::GetPBMessage() const
{
    return mPBMessage;
}


//OTSStringValueImpl
OTSStringValueImpl::OTSStringValueImpl(
            cloudservice::ots2::ColumnValue* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
    mPBMessage->set_type(cloudservice::ots2::STRING);
}

OTSStringValueImpl::~OTSStringValueImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

ColumnType OTSStringValueImpl::GetType() const
{
    return STRING;
}

const string& OTSStringValueImpl::GetValue() const
{
    if (!mPBMessage->has_v_string()) {
        OTS_THROW("OTSClient", "ClientInternalError", "ColumnValue is null");
    }
    return mPBMessage->v_string();
}

void OTSStringValueImpl::SetValue(const string& value)
{
    mPBMessage->set_v_string(value);
}

Message* OTSStringValueImpl::GetPBMessage() const
{
    return mPBMessage;
}

//OTSIntValueImpl
OTSIntValueImpl::OTSIntValueImpl(
            cloudservice::ots2::ColumnValue* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
    mPBMessage->set_type(cloudservice::ots2::INTEGER);
}

OTSIntValueImpl::~OTSIntValueImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

ColumnType OTSIntValueImpl::GetType() const
{
    return INTEGER;
}

int64_t OTSIntValueImpl::GetValue() const
{
    if (!mPBMessage->has_v_int()) {
        OTS_THROW("OTSClient", "ClientInternalError", "ColumnValue is null");
    }
    return mPBMessage->v_int();
}

void OTSIntValueImpl::SetValue(int64_t value)
{
    mPBMessage->set_v_int(value);
}

Message* OTSIntValueImpl::GetPBMessage() const
{
    return mPBMessage;
}


//OTSBoolValueImpl
OTSBoolValueImpl::OTSBoolValueImpl(
            cloudservice::ots2::ColumnValue* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
    mPBMessage->set_type(cloudservice::ots2::BOOLEAN);
}

OTSBoolValueImpl::~OTSBoolValueImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

ColumnType OTSBoolValueImpl::GetType() const
{
    return BOOLEAN;
}

bool OTSBoolValueImpl::GetValue() const
{
    if (!mPBMessage->has_v_bool()) {
        OTS_THROW("OTSClient", "ClientInternalError", "ColumnValue is null");
    }
    return mPBMessage->v_bool();
}

void OTSBoolValueImpl::SetValue(bool value)
{
    mPBMessage->set_v_bool(value);
}

Message* OTSBoolValueImpl::GetPBMessage() const
{
    return mPBMessage;
}


//OTSDoubleValueImpl
OTSDoubleValueImpl::OTSDoubleValueImpl(
            cloudservice::ots2::ColumnValue* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
    mPBMessage->set_type(cloudservice::ots2::DOUBLE);
}

OTSDoubleValueImpl::~OTSDoubleValueImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

ColumnType OTSDoubleValueImpl::GetType() const
{
    return DOUBLE;
}

double OTSDoubleValueImpl::GetValue() const
{
    if (!mPBMessage->has_v_double()) {
        OTS_THROW("OTSClient", "ClientInternalError", "ColumnValue is null");
    }
    return mPBMessage->v_double();
}

void OTSDoubleValueImpl::SetValue(double value)
{
    mPBMessage->set_v_double(value);
}

Message* OTSDoubleValueImpl::GetPBMessage() const
{
    return mPBMessage;
}


// OTSBinaryValueImpl
OTSBinaryValueImpl::OTSBinaryValueImpl(
            cloudservice::ots2::ColumnValue* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
    mPBMessage->set_type(cloudservice::ots2::BINARY);
}

OTSBinaryValueImpl::~OTSBinaryValueImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

ColumnType OTSBinaryValueImpl::GetType() const
{
    return BINARY;
}

void OTSBinaryValueImpl::GetValue(char** data, int32_t* size) const
{
    if (!mPBMessage->has_v_binary()) {
        OTS_THROW("OTSClient", "ClientInternalError", "ColumnValue is null");
    }
    const string& value = mPBMessage->v_binary();
    *data = const_cast<char*>(value.data());
    *size = static_cast<int32_t>(value.size());
}

void OTSBinaryValueImpl::SetValue(const char* data, int32_t size)
{
    if (data == NULL || size < 0) {
        OTS_THROW("OTSClient", "ClientInternalError", "Data or Size is invalid");
    }
    mPBMessage->set_v_binary((const void*)data, size);
}

Message* OTSBinaryValueImpl::GetPBMessage() const
{
    return mPBMessage;
}


//ColumnSchemaImpl
ColumnSchemaImpl::ColumnSchemaImpl(
            cloudservice::ots2::ColumnSchema* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

ColumnSchemaImpl::~ColumnSchemaImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

const string& ColumnSchemaImpl::GetColumnName() const
{
    return mPBMessage->name();
}

ColumnType ColumnSchemaImpl::GetColumnType() const
{
    cloudservice::ots2::ColumnType type = mPBMessage->type();
    switch (type)
    {
        case cloudservice::ots2::STRING:
            return STRING;
        case cloudservice::ots2::INTEGER:
            return INTEGER;
        case cloudservice::ots2::BOOLEAN:
            return BOOLEAN;
        case cloudservice::ots2::DOUBLE:
            return DOUBLE;
        case cloudservice::ots2::BINARY:
            return BINARY;
        default:
            OTS_THROW("OTSClient", "ClientInternalError", "The type of schema is invalid");
    }
}

void ColumnSchemaImpl::SetColumnName(const string& name)
{
    mPBMessage->set_name(name);
}

void ColumnSchemaImpl::SetColumnType(ColumnType type)
{
    switch (type)
    {
        case STRING:
            mPBMessage->set_type(cloudservice::ots2::STRING);
            break;
        case INTEGER:
            mPBMessage->set_type(cloudservice::ots2::INTEGER);
            break;
        case BOOLEAN:
            mPBMessage->set_type(cloudservice::ots2::BOOLEAN);
            break;
        case DOUBLE:
            mPBMessage->set_type(cloudservice::ots2::DOUBLE);
            break;
        case BINARY:
            mPBMessage->set_type(cloudservice::ots2::BINARY);
            break;
        default:
            OTS_THROW("OTSClient", "ClientInternalError", "The type of column is invalid");
    }
}

Message* ColumnSchemaImpl::GetPBMessage() const
{
    return mPBMessage;
}

//ColumnImpl
ColumnImpl::ColumnImpl(
            cloudservice::ots2::Column* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message), mColumnValue(NULL)
{
}

ColumnImpl::~ColumnImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mColumnValue;;
}

const string& ColumnImpl::GetColumnName() const
{
    return mPBMessage->name();
}

const OTSValue& ColumnImpl::GetColumnValue() const
{
    if (NULL == mColumnValue) {
        OTS_THROW("OTSClient", "ClientInternalError", "ColumnValue is null");
    }
    return *mColumnValue;
}

void ColumnImpl::SetColumnName(const std::string& name)
{
    mPBMessage->set_name(name);
}
    
void ColumnImpl::SetColumnValue(OTSValue* value)
{
    if (NULL == value) {
        OTS_THROW("OTSClient", "OTSParameterInvalid", "ColumnValue is null");
    }
    cloudservice::ots2::ColumnValue* pbValue = mPBMessage->mutable_value();  
    OTSValueImpl* valueImpl = dynamic_cast<OTSValueImpl*>(value);
    cloudservice::ots2::ColumnValue* pbRealValue = 
        dynamic_cast<cloudservice::ots2::ColumnValue*>(valueImpl->GetPBMessage());
    pbValue->Swap(pbRealValue);
    delete value;
    // Make sure the value could be accessed by GetColumnValue() 
    ParseFromPBMessage();
}

void ColumnImpl::ParseFromPBMessage()
{
    cloudservice::ots2::ColumnValue* pbValue = mPBMessage->mutable_value();
    cloudservice::ots2::ColumnType type = pbValue->type();;
    switch (type)
    {
        case cloudservice::ots2::STRING:
            mColumnValue = OTSFactoryImpl::NewStringValue(pbValue);
            break;
        case cloudservice::ots2::INTEGER:
            mColumnValue = OTSFactoryImpl::NewIntValue(pbValue);
            break;
        case cloudservice::ots2::BOOLEAN:
            mColumnValue = OTSFactoryImpl::NewBoolValue(pbValue);
            break;
        case cloudservice::ots2::DOUBLE:
            mColumnValue = OTSFactoryImpl::NewDoubleValue(pbValue);
            break;
        case cloudservice::ots2::BINARY:
            mColumnValue = OTSFactoryImpl::NewBinaryValue(pbValue);
            break;
        case cloudservice::ots2::INF_MIN:
            mColumnValue = OTSFactoryImpl::NewInfMinValue(pbValue);
            break;
        case cloudservice::ots2::INF_MAX:
            mColumnValue = OTSFactoryImpl::NewInfMaxValue(pbValue);
            break;
    }
}

Message* ColumnImpl::GetPBMessage() const
{
    return mPBMessage;
}


//ColumnUpdateImpl
ColumnUpdateImpl::ColumnUpdateImpl(
            cloudservice::ots2::ColumnUpdate* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

ColumnUpdateImpl::~ColumnUpdateImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void ColumnUpdateImpl::SetOperationType(OperationType type)
{
    switch (type)
    {
        case PUT:
            mPBMessage->set_type(cloudservice::ots2::PUT);
            break;
        case DELETE:
            mPBMessage->set_type(cloudservice::ots2::DELETE);
            break;
        default:
            OTS_THROW("OTSClient", "ClientInternalError", "The type of operation is invalid");
    }
}

void ColumnUpdateImpl::SetColumnName(const std::string& name)
{
    mPBMessage->set_name(name);
}
    
void ColumnUpdateImpl::SetColumnValue(OTSValue* value)
{
    if (NULL == value) {
        OTS_THROW("OTSClient", "ClientParameterError", "ColumnValue is invalid");
    }
    cloudservice::ots2::ColumnValue* pbValue = mPBMessage->mutable_value();  
    OTSValueImpl* valueImpl = dynamic_cast<OTSValueImpl*>(value);
    cloudservice::ots2::ColumnValue* pbRealValue = 
        dynamic_cast<cloudservice::ots2::ColumnValue*>(valueImpl->GetPBMessage());
    pbValue->Swap(pbRealValue);
    delete value;
}

Message* ColumnUpdateImpl::GetPBMessage() const
{
    return mPBMessage;
}


//RowImpl
RowImpl::RowImpl(
            cloudservice::ots2::Row* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message) 
{
}

RowImpl::~RowImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    for (size_t i = 0; i < mPrimaryKeys.size(); ++i) {
        delete mPrimaryKeys[i];
    }
    for (size_t i = 0; i < mColumns.size(); ++i) {
        delete mColumns[i];
    }
}

int32_t RowImpl::GetPrimaryKeySize() const
{
    return mPrimaryKeys.size();
}

const Column& RowImpl::GetPrimaryKey(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mPrimaryKeys.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mPrimaryKeys[index];
}

int32_t RowImpl::GetColumnSize() const
{
    return mColumns.size();
}

const Column& RowImpl::GetColumn(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mColumns.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mColumns[index];
}

void RowImpl::ParseFromPBMessage()
{
    int32_t pkSize = mPBMessage->primary_key_columns_size();
    for (int32_t i = 0; i < pkSize; ++i) {
        cloudservice::ots2::Column* pbColumn = mPBMessage->mutable_primary_key_columns(i);
        ColumnImpl* column = dynamic_cast<ColumnImpl*>(OTSFactoryImpl::NewColumn(pbColumn));
        column->ParseFromPBMessage();
        mPrimaryKeys.push_back(column);
    }
    int32_t colSize = mPBMessage->attribute_columns_size();
    for (int32_t i = 0; i < colSize; ++i) {
        cloudservice::ots2::Column* pbColumn = mPBMessage->mutable_attribute_columns(i);
        ColumnImpl* column = dynamic_cast<ColumnImpl*>(OTSFactoryImpl::NewColumn(pbColumn));
        column->ParseFromPBMessage();
        mColumns.push_back(column);
    }
}

Message* RowImpl::GetPBMessage() const
{
    return mPBMessage;
}

// TableMetaImpl
TableMetaImpl::TableMetaImpl(
            cloudservice::ots2::TableMeta* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

TableMetaImpl::~TableMetaImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    for (int32_t i = 0; i < (int32_t)mPrimaryKeySchemas.size(); ++i) {
        delete mPrimaryKeySchemas[i];
    }
}

void TableMetaImpl::SetTableName(const string& tableName)
{
    mPBMessage->set_table_name(tableName);
}

void TableMetaImpl::AddPrimaryKeySchema(ColumnSchema* schema)
{
    if (schema == NULL) {
        OTS_THROW("OTSClient", "ClientParameterError", "ColumnSchem is null");
    }
    cloudservice::ots2::ColumnSchema* pbSchema = mPBMessage->add_primary_key();
    ColumnSchemaImpl* schemaImpl = dynamic_cast<ColumnSchemaImpl*>(schema);
    pbSchema->Swap(dynamic_cast<cloudservice::ots2::ColumnSchema*>(schemaImpl->GetPBMessage()));
    delete schema;

    // If user adds a primary key schema, make sure that the schema can be got.
    ColumnSchema* newSchema = OTSFactoryImpl::NewColumnSchema(pbSchema);
    mPrimaryKeySchemas.push_back(newSchema);
}

const string& TableMetaImpl::GetTableName() const
{
    return mPBMessage->table_name();
}

int32_t TableMetaImpl::GetPrimaryKeySchemaSize() const
{
    return mPrimaryKeySchemas.size();
}

const ColumnSchema& TableMetaImpl::GetPrimaryKeySchema(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mPrimaryKeySchemas.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mPrimaryKeySchemas[index];
}

void TableMetaImpl::ParseFromPBMessage()
{
    int32_t schemaSize = mPBMessage->primary_key_size();
    for (int32_t i = 0; i < schemaSize; ++i) {
        cloudservice::ots2::ColumnSchema* pbSchema = mPBMessage->mutable_primary_key(i);
        ColumnSchema* schema = OTSFactoryImpl::NewColumnSchema(pbSchema);
        mPrimaryKeySchemas.push_back(schema);
    }
}

Message* TableMetaImpl::GetPBMessage() const
{
    return mPBMessage;
}


// CreateTableRequestImpl
CreateTableRequestImpl::CreateTableRequestImpl(
            cloudservice::ots2::CreateTableRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

CreateTableRequestImpl::~CreateTableRequestImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void CreateTableRequestImpl::SetTableMeta(TableMeta* tableMeta)
{
    if (tableMeta == NULL) {
        OTS_THROW("OTSClient", "ClientParameterError", "TableMeta is null");
    }
    cloudservice::ots2::TableMeta* pbTableMeta = mPBMessage->mutable_table_meta();
    TableMetaImpl* tableMetaImpl = dynamic_cast<TableMetaImpl*>(tableMeta);
    pbTableMeta->Swap(dynamic_cast<cloudservice::ots2::TableMeta*>(tableMetaImpl->GetPBMessage()));
    delete tableMeta;
}

void CreateTableRequestImpl::SetReservedThroughput(ReservedThroughput* throughput)
{
    if (throughput == NULL) {
        OTS_THROW("OTSClient", "ClientParameterError", "ReservedThroughput is null");
    }
    cloudservice::ots2::ReservedThroughput* pbThroughput = mPBMessage->mutable_reserved_throughput();
    ReservedThroughputImpl* throughputImpl = dynamic_cast<ReservedThroughputImpl*>(throughput);
    pbThroughput->Swap(dynamic_cast<cloudservice::ots2::ReservedThroughput*>(throughputImpl->GetPBMessage()));
    delete throughput;
}

Message* CreateTableRequestImpl::GetPBMessage() const
{
    return mPBMessage;
}


//UpdateTableRequestImpl
UpdateTableRequestImpl::UpdateTableRequestImpl(
            cloudservice::ots2::UpdateTableRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

UpdateTableRequestImpl::~UpdateTableRequestImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void UpdateTableRequestImpl::SetTableName(const string& tableName)
{
    mPBMessage->set_table_name(tableName);
}

void UpdateTableRequestImpl::SetReservedThroughput(ReservedThroughput* throughput)
{
    if (throughput == NULL) {
        OTS_THROW("OTSClient", "ClientParameterError", "ReservedThroughput is null");
    }
    cloudservice::ots2::ReservedThroughput* pbThroughput = mPBMessage->mutable_reserved_throughput();
    ReservedThroughputImpl* throughputImpl = dynamic_cast<ReservedThroughputImpl*>(throughput);
    pbThroughput->Swap(dynamic_cast<cloudservice::ots2::ReservedThroughput*>(throughputImpl->GetPBMessage()));
    delete throughput;
}

Message* UpdateTableRequestImpl::GetPBMessage() const
{
    return mPBMessage;
}


//UpdateTableResponseImpl
UpdateTableResponseImpl::UpdateTableResponseImpl(
            cloudservice::ots2::UpdateTableResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

UpdateTableResponseImpl::~UpdateTableResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

const ReservedThroughputDetails& UpdateTableResponseImpl::GetReservedThroughputDetails() const
{
    return *mReservedThroughputDetails;
}

Message* UpdateTableResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}

//DescribeTableResponseImpl
DescribeTableResponseImpl::DescribeTableResponseImpl(
            cloudservice::ots2::DescribeTableResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

DescribeTableResponseImpl::~DescribeTableResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mTableMeta;
    delete mReservedThroughputDetails;
}

const TableMeta& DescribeTableResponseImpl::GetTableMeta() const
{
    return *mTableMeta;
}

const ReservedThroughputDetails& DescribeTableResponseImpl::GetReservedThroughputDetails() const
{
    return *mReservedThroughputDetails;
}

void DescribeTableResponseImpl::ParseFromPBMessage()
{
    // parse TableMeta
    cloudservice::ots2::TableMeta* pbTableMeta = mPBMessage->mutable_table_meta();
    mTableMeta = OTSFactoryImpl::NewTableMeta(pbTableMeta);
    TableMetaImpl* tableMetaImpl = dynamic_cast<TableMetaImpl*>(mTableMeta);
    tableMetaImpl->ParseFromPBMessage();

    // parse CapacityUnit
    cloudservice::ots2::ReservedThroughputDetails* pbCapacity = mPBMessage->mutable_reserved_throughput_details();
    mReservedThroughputDetails = OTSFactoryImpl::NewReservedThroughputDetails(pbCapacity);
    ReservedThroughputDetailsImpl* detailsImpl = dynamic_cast<ReservedThroughputDetailsImpl*>(mReservedThroughputDetails);
    detailsImpl->ParseFromPBMessage();
}

Message* DescribeTableResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}


//ListTableResponseImpl
ListTableResponseImpl::ListTableResponseImpl(
            cloudservice::ots2::ListTableResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

ListTableResponseImpl::~ListTableResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

int32_t ListTableResponseImpl::GetTableNameSize() const
{
    return mPBMessage->table_names_size();
}

const string& ListTableResponseImpl::GetTableName(int32_t index) const
{
    if (index < 0 || index >= mPBMessage->table_names_size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return mPBMessage->table_names(index);
}

Message* ListTableResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}


//GetRowRequestImpl
GetRowRequestImpl::GetRowRequestImpl(
            cloudservice::ots2::GetRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

GetRowRequestImpl::~GetRowRequestImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void GetRowRequestImpl::SetTableName(const string& tableName)
{
    mPBMessage->set_table_name(tableName);
}

void GetRowRequestImpl::AddPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_primary_key();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

void GetRowRequestImpl::AddColumnName(const string& name)
{
    mPBMessage->add_columns_to_get(name); 
}

Message* GetRowRequestImpl::GetPBMessage() const
{
    return mPBMessage;
}


//GetRowResponseImpl
GetRowResponseImpl::GetRowResponseImpl(
            cloudservice::ots2::GetRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message), 
    mRow(NULL), mConsumedCapacity(NULL)
{
}

GetRowResponseImpl::~GetRowResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mRow; 
    delete mConsumedCapacity; 
}

const Row& GetRowResponseImpl::GetRow() const
{
    if (NULL == mRow) {
        OTS_THROW("OTClient", "ClientInternalError", "Row is null");
    }
    return *mRow;
}

const CapacityUnit& GetRowResponseImpl::GetConsumedCapacity() const
{
    if (NULL == mConsumedCapacity) {
        OTS_THROW("OTClient", "ClientInternalError", "CapacityUnit is null");
    }
    return *mConsumedCapacity;
}

void GetRowResponseImpl::ParseFromPBMessage()
{
    // Parse CapacityUnit
    cloudservice::ots2::ConsumedCapacity* pbConsumed = mPBMessage->mutable_consumed();
    cloudservice::ots2::CapacityUnit* pbCapacity = pbConsumed->mutable_capacity_unit();
    mConsumedCapacity = OTSFactoryImpl::NewCapacityUnit(pbCapacity);

    // Parse Row
    cloudservice::ots2::Row* pbRow = mPBMessage->mutable_row();
    mRow = OTSFactoryImpl::NewRow(pbRow);
    RowImpl* rowImpl = dynamic_cast<RowImpl*>(mRow);
    rowImpl->ParseFromPBMessage();
}

Message* GetRowResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}


//ConditionImpl
ConditionImpl::ConditionImpl(
            cloudservice::ots2::Condition* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

ConditionImpl::~ConditionImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void ConditionImpl::SetRowExistenceExpectation(RowExistenceExpectation expect)
{
    switch (expect)
    {
        case IGNORE:
            mPBMessage->set_row_existence(cloudservice::ots2::IGNORE);
            break;;
        case EXPECT_EXIST:
            mPBMessage->set_row_existence(cloudservice::ots2::EXPECT_EXIST);
            break;
        case EXPECT_NOT_EXIST:
            mPBMessage->set_row_existence(cloudservice::ots2::EXPECT_NOT_EXIST);
            break;
    }
}

Message* ConditionImpl::GetPBMessage() const
{
    return mPBMessage;
}


// PutRowRequestImpl
PutRowRequestImpl::PutRowRequestImpl(
            cloudservice::ots2::PutRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

PutRowRequestImpl::~PutRowRequestImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void PutRowRequestImpl::SetTableName(const string& tableName)
{
    mPBMessage->set_table_name(tableName);
}

void PutRowRequestImpl::SetCondition(Condition* condition)
{
    if (NULL == condition) {
        OTS_THROW("OTSClient", "ClientParameterError", "Condition is null");
    }
    cloudservice::ots2::Condition* pbCondition = mPBMessage->mutable_condition();
    ConditionImpl* conditionImpl = dynamic_cast<ConditionImpl*>(condition);
    pbCondition->Swap(dynamic_cast<cloudservice::ots2::Condition*>(conditionImpl->GetPBMessage()));
    delete condition;
}

void PutRowRequestImpl::AddPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_primary_key();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

void PutRowRequestImpl::AddColumn(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Column is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_attribute_columns();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

Message* PutRowRequestImpl::GetPBMessage() const
{
    return mPBMessage;
}


// PutRowResponseImpl
PutRowResponseImpl::PutRowResponseImpl(
            cloudservice::ots2::PutRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message), mConsumedCapacity(NULL)
{
}

PutRowResponseImpl::~PutRowResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mConsumedCapacity;
}

const CapacityUnit& PutRowResponseImpl::GetConsumedCapacity() const
{
    if (NULL == mConsumedCapacity) {
        OTS_THROW("OTSClient", "ClientInternalError", "CapacityUnit is null");
    }
    return *mConsumedCapacity;
}

void PutRowResponseImpl::ParseFromPBMessage()
{
    cloudservice::ots2::ConsumedCapacity* pbConsumed = mPBMessage->mutable_consumed();
    cloudservice::ots2::CapacityUnit* pbCapacity = pbConsumed->mutable_capacity_unit();
    mConsumedCapacity = OTSFactoryImpl::NewCapacityUnit(pbCapacity);
}

Message* PutRowResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}


//UpdateRowRequestImpl
UpdateRowRequestImpl::UpdateRowRequestImpl(
            cloudservice::ots2::UpdateRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

UpdateRowRequestImpl::~UpdateRowRequestImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void UpdateRowRequestImpl::SetTableName(const string& tableName)
{
    mPBMessage->set_table_name(tableName);
}

void UpdateRowRequestImpl::SetCondition(Condition* condition)
{
    if (NULL == condition) {
        OTS_THROW("OTSClient", "ClientParameterError", "Condition is null");
    }
    cloudservice::ots2::Condition* pbCondition = mPBMessage->mutable_condition();
    ConditionImpl* conditionImpl = dynamic_cast<ConditionImpl*>(condition);
    pbCondition->Swap(dynamic_cast<cloudservice::ots2::Condition*>(conditionImpl->GetPBMessage()));
    delete condition;
}

void UpdateRowRequestImpl::AddPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_primary_key();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

void UpdateRowRequestImpl::AddColumn(ColumnUpdate* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Column is null");
    }
    cloudservice::ots2::ColumnUpdate* pbColumn = mPBMessage->add_attribute_columns();
    ColumnUpdateImpl* columnImpl = dynamic_cast<ColumnUpdateImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::ColumnUpdate*>(columnImpl->GetPBMessage()));
    delete column;
}

Message* UpdateRowRequestImpl::GetPBMessage() const
{
    return mPBMessage;
}


//UpdateRowResponseImpl
UpdateRowResponseImpl::UpdateRowResponseImpl(
            cloudservice::ots2::UpdateRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message), mConsumedCapacity(NULL)
{
}

UpdateRowResponseImpl::~UpdateRowResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mConsumedCapacity;
}

const CapacityUnit& UpdateRowResponseImpl::GetConsumedCapacity() const
{
    if (NULL == mConsumedCapacity) {
        OTS_THROW("OTSClient", "ClientInternalError", "CapacityUnit is null");
    }
    return *mConsumedCapacity;
}

void UpdateRowResponseImpl::ParseFromPBMessage()
{
    cloudservice::ots2::ConsumedCapacity* pbConsumed = mPBMessage->mutable_consumed();
    cloudservice::ots2::CapacityUnit* pbCapacity = pbConsumed->mutable_capacity_unit();
    mConsumedCapacity = OTSFactoryImpl::NewCapacityUnit(pbCapacity);
}

Message* UpdateRowResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}


//DeleteRowRequestImpl
DeleteRowRequestImpl::DeleteRowRequestImpl(
            cloudservice::ots2::DeleteRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

DeleteRowRequestImpl::~DeleteRowRequestImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void DeleteRowRequestImpl::SetTableName(const string& tableName)
{
    mPBMessage->set_table_name(tableName);
}

void DeleteRowRequestImpl::SetCondition(Condition* condition)
{
    if (NULL == condition) {
        OTS_THROW("OTSClient", "ClientParameterError", "Condition is null");
    }
    cloudservice::ots2::Condition* pbCondition = mPBMessage->mutable_condition();
    ConditionImpl* conditionImpl = dynamic_cast<ConditionImpl*>(condition);
    pbCondition->Swap(dynamic_cast<cloudservice::ots2::Condition*>(conditionImpl->GetPBMessage()));
    delete condition;
}

void DeleteRowRequestImpl::AddPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_primary_key();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

Message* DeleteRowRequestImpl::GetPBMessage() const
{
    return mPBMessage;
}


//DeleteRowResponseImpl
DeleteRowResponseImpl::DeleteRowResponseImpl(
            cloudservice::ots2::DeleteRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message), mConsumedCapacity(NULL)
{
}

DeleteRowResponseImpl::~DeleteRowResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mConsumedCapacity;
}

const CapacityUnit& DeleteRowResponseImpl::GetConsumedCapacity() const
{
    if (NULL == mConsumedCapacity) {
        OTS_THROW("OTSClient", "ClientInternalError", "CapacityUnit is null");
    }
    return *mConsumedCapacity;
}

void DeleteRowResponseImpl::ParseFromPBMessage()
{
    cloudservice::ots2::ConsumedCapacity* pbConsumed = mPBMessage->mutable_consumed();
    cloudservice::ots2::CapacityUnit* pbCapacity = pbConsumed->mutable_capacity_unit();
    mConsumedCapacity = OTSFactoryImpl::NewCapacityUnit(pbCapacity);
}

Message* DeleteRowResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchGetRequestRowItemImpl
BatchGetRequestRowItemImpl::BatchGetRequestRowItemImpl(
            cloudservice::ots2::RowInBatchGetRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchGetRequestRowItemImpl::~BatchGetRequestRowItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void BatchGetRequestRowItemImpl::AddPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_primary_key();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

Message* BatchGetRequestRowItemImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchGetRequestTableItemImpl
BatchGetRequestTableItemImpl::BatchGetRequestTableItemImpl(
            cloudservice::ots2::TableInBatchGetRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchGetRequestTableItemImpl::~BatchGetRequestTableItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void BatchGetRequestTableItemImpl::SetTableName(const string& tableName)
{
    mPBMessage->set_table_name(tableName);
}

void BatchGetRequestTableItemImpl::AddRowItem(BatchGetRequestRowItem* rowItem)
{
    if (NULL == rowItem) {
        OTS_THROW("OTSClient", "ClientParameterError", "BatchGetResponseRowItem is null");
    }
    cloudservice::ots2::RowInBatchGetRowRequest* pbRow = mPBMessage->add_rows();
    BatchGetRequestRowItemImpl* rowImpl = 
        dynamic_cast<BatchGetRequestRowItemImpl*>(rowItem);
    pbRow->Swap(dynamic_cast<cloudservice::ots2::RowInBatchGetRowRequest*>(rowImpl->GetPBMessage()));
    delete rowItem;
}

void BatchGetRequestTableItemImpl::AddColumnName(const string& name)
{
    mPBMessage->add_columns_to_get(name);
}

Message* BatchGetRequestTableItemImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchGetRowRequestImpl
BatchGetRowRequestImpl::BatchGetRowRequestImpl(
            cloudservice::ots2::BatchGetRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchGetRowRequestImpl::~BatchGetRowRequestImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void BatchGetRowRequestImpl::AddTableItem(BatchGetRequestTableItem* tableItem)
{
    if (NULL == tableItem) {
        OTS_THROW("OTSClient", "ClientParameterError", "BatchGetRequestTableItem is null");
    }
    cloudservice::ots2::TableInBatchGetRowRequest* pbTable = mPBMessage->add_tables();
    BatchGetRequestTableItemImpl* tableImpl = 
        dynamic_cast<BatchGetRequestTableItemImpl*>(tableItem);
    pbTable->Swap(dynamic_cast<cloudservice::ots2::TableInBatchGetRowRequest*>(tableImpl->GetPBMessage()));
    delete tableItem;
}

Message* BatchGetRowRequestImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchGetResponseRowItemImpl
BatchGetResponseRowItemImpl::BatchGetResponseRowItemImpl(
            cloudservice::ots2::RowInBatchGetRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message),
    mError(NULL), mConsumedCapacity(NULL), mRow(NULL)
{
}

BatchGetResponseRowItemImpl::~BatchGetResponseRowItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mError;
    delete mConsumedCapacity;
    delete mRow;
}

bool BatchGetResponseRowItemImpl::IsOK() const
{
    return mPBMessage->is_ok();
}

const Error& BatchGetResponseRowItemImpl::GetError() const
{
    if (mPBMessage->is_ok()) {
        OTS_THROW("OTSClient", "ClientParameterError", "The row's result is ok");
    }
    if (NULL == mError) {
        OTS_THROW("OTSClient", "ClientInternalError", "Error is null");
    }
    return *mError;
}

const CapacityUnit& BatchGetResponseRowItemImpl::GetConsumedCapacity() const
{
    if (!mPBMessage->is_ok()) {
        OTS_THROW("OTSClient", "ClientParameterError", "The row's result is not ok");
    }
    if (NULL == mConsumedCapacity) {
        OTS_THROW("OTSClient", "ClientInternalError", "CapacityUnit is null");
    }
    return *mConsumedCapacity;
}

const Row& BatchGetResponseRowItemImpl::GetRow() const
{
    if (!mPBMessage->is_ok()) {
        OTS_THROW("OTSClient", "ClientParameterError", "The row's result is not ok");
    }
    if (NULL == mRow) {
        OTS_THROW("OTSClient", "ClientInternalError", "Row is null");
    }
    return *mRow;
}

void BatchGetResponseRowItemImpl::ParseFromPBMessage()
{
    if (!mPBMessage->is_ok()) {
        cloudservice::ots2::Error* pbError = mPBMessage->mutable_error();
        mError = OTSFactoryImpl::NewError(pbError);
    } else {
        // Parse Row
        cloudservice::ots2::Row* pbRow = mPBMessage->mutable_row();
        mRow = OTSFactoryImpl::NewRow(pbRow);
        RowImpl* rowImpl = dynamic_cast<RowImpl*>(mRow);
        rowImpl->ParseFromPBMessage();

        // Parse CapacityUnit
        cloudservice::ots2::ConsumedCapacity* pbConsumed = mPBMessage->mutable_consumed();
        cloudservice::ots2::CapacityUnit* pbCapacity = pbConsumed->mutable_capacity_unit();
        mConsumedCapacity = OTSFactoryImpl::NewCapacityUnit(pbCapacity);
    }
}

Message* BatchGetResponseRowItemImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchGetResponseTableItemImpl
BatchGetResponseTableItemImpl::BatchGetResponseTableItemImpl(
            cloudservice::ots2::TableInBatchGetRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchGetResponseTableItemImpl::~BatchGetResponseTableItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    for (int32_t i = 0; i < (int32_t)mRowItems.size(); ++i) {
        delete mRowItems[i];
    }
}

const string& BatchGetResponseTableItemImpl::GetTableName() const
{
    return mPBMessage->table_name();
}

int32_t BatchGetResponseTableItemImpl::GetRowSize() const
{
    return mRowItems.size();
}

const BatchGetResponseRowItem& BatchGetResponseTableItemImpl::GetRowItem(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mRowItems.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mRowItems[index];
}

void BatchGetResponseTableItemImpl::ParseFromPBMessage()
{
    int32_t rowSize = mPBMessage->rows_size();
    for (int32_t i = 0; i < rowSize; ++i) {
        cloudservice::ots2::RowInBatchGetRowResponse* pbRow = mPBMessage->mutable_rows(i);
        BatchGetResponseRowItem* row = OTSFactoryImpl::NewBatchGetResponseRowItem(pbRow);
        BatchGetResponseRowItemImpl* rowImpl = dynamic_cast<BatchGetResponseRowItemImpl*>(row);
        rowImpl->ParseFromPBMessage();
        mRowItems.push_back(row);
    }
}

Message* BatchGetResponseTableItemImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchGetRowResponseImpl
BatchGetRowResponseImpl::BatchGetRowResponseImpl(
            cloudservice::ots2::BatchGetRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchGetRowResponseImpl::~BatchGetRowResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    for (int32_t i = 0; i < (int32_t)mTableItems.size(); ++i) {
        delete mTableItems[i];
    }
}

int32_t BatchGetRowResponseImpl::GetTableItemSize() const
{
    return mTableItems.size();    
}

const BatchGetResponseTableItem& BatchGetRowResponseImpl::GetTableItem(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mTableItems.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mTableItems[index];
}

void BatchGetRowResponseImpl::ParseFromPBMessage()
{
    int32_t tableSize = mPBMessage->tables_size();
    for (int32_t i = 0; i < tableSize; ++i) {
        cloudservice::ots2::TableInBatchGetRowResponse* pbTable = mPBMessage->mutable_tables(i);
        BatchGetResponseTableItem* table = OTSFactoryImpl::NewBatchGetResponseTableItem(pbTable);
        BatchGetResponseTableItemImpl* tableImpl = dynamic_cast<BatchGetResponseTableItemImpl*>(table);
        tableImpl->ParseFromPBMessage();
        mTableItems.push_back(table);
    }
}

Message* BatchGetRowResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchWriteRequestPutRowItemImpl
BatchWriteRequestPutRowItemImpl::BatchWriteRequestPutRowItemImpl(
            cloudservice::ots2::PutRowInBatchWriteRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchWriteRequestPutRowItemImpl::~BatchWriteRequestPutRowItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void BatchWriteRequestPutRowItemImpl::SetCondition(Condition* condition)
{
    if (NULL == condition) {
        OTS_THROW("OTSClient", "ClientParameterError", "Condition is null");
    }
    cloudservice::ots2::Condition* pbCondition = mPBMessage->mutable_condition();
    ConditionImpl* conditionImpl = dynamic_cast<ConditionImpl*>(condition);
    pbCondition->Swap(dynamic_cast<cloudservice::ots2::Condition*>(conditionImpl->GetPBMessage()));
    delete condition;
}

void BatchWriteRequestPutRowItemImpl::AddPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_primary_key();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

void BatchWriteRequestPutRowItemImpl::AddColumn(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Column is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_attribute_columns();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

Message* BatchWriteRequestPutRowItemImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchWriteRequestUpdateRowItemImpl
BatchWriteRequestUpdateRowItemImpl::BatchWriteRequestUpdateRowItemImpl(
            cloudservice::ots2::UpdateRowInBatchWriteRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchWriteRequestUpdateRowItemImpl::~BatchWriteRequestUpdateRowItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void BatchWriteRequestUpdateRowItemImpl::SetCondition(Condition* condition)
{
    if (NULL == condition) {
        OTS_THROW("OTSClient", "ClientParameterError", "Condition is null");
    }
    cloudservice::ots2::Condition* pbCondition = mPBMessage->mutable_condition();
    ConditionImpl* conditionImpl = dynamic_cast<ConditionImpl*>(condition);
    pbCondition->Swap(dynamic_cast<cloudservice::ots2::Condition*>(conditionImpl->GetPBMessage()));
    delete condition;
}

void BatchWriteRequestUpdateRowItemImpl::AddPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_primary_key();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

void BatchWriteRequestUpdateRowItemImpl::AddColumn(ColumnUpdate* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Column is null");
    }
    cloudservice::ots2::ColumnUpdate* pbColumn = mPBMessage->add_attribute_columns();
    ColumnUpdateImpl* columnImpl = dynamic_cast<ColumnUpdateImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::ColumnUpdate*>(columnImpl->GetPBMessage()));
    delete column;
}

Message* BatchWriteRequestUpdateRowItemImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchWriteRequestDeleteRowItemImpl
BatchWriteRequestDeleteRowItemImpl::BatchWriteRequestDeleteRowItemImpl(
            cloudservice::ots2::DeleteRowInBatchWriteRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchWriteRequestDeleteRowItemImpl::~BatchWriteRequestDeleteRowItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void BatchWriteRequestDeleteRowItemImpl::SetCondition(Condition* condition)
{
    if (NULL == condition) {
        OTS_THROW("OTSClient", "ClientParameterError", "Condition is null");
    }
    cloudservice::ots2::Condition* pbCondition = mPBMessage->mutable_condition();
    ConditionImpl* conditionImpl = dynamic_cast<ConditionImpl*>(condition);
    pbCondition->Swap(dynamic_cast<cloudservice::ots2::Condition*>(conditionImpl->GetPBMessage()));
    delete condition;
}

void BatchWriteRequestDeleteRowItemImpl::AddPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_primary_key();
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

Message* BatchWriteRequestDeleteRowItemImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchWriteRequestTableItemImpl
BatchWriteRequestTableItemImpl::BatchWriteRequestTableItemImpl(
            cloudservice::ots2::TableInBatchWriteRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchWriteRequestTableItemImpl::~BatchWriteRequestTableItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void BatchWriteRequestTableItemImpl::SetTableName(const string& tableName)
{
    mPBMessage->set_table_name(tableName);
}

void BatchWriteRequestTableItemImpl::AddPutRowItem(BatchWriteRequestPutRowItem* rowItem)
{
    if (NULL == rowItem) {
        OTS_THROW("OTSClient", "ClientParameterError", "BatchWriteRequestPutRowItem is null");
    }
    cloudservice::ots2::PutRowInBatchWriteRowRequest* pbRow = mPBMessage->add_put_rows();
    BatchWriteRequestPutRowItemImpl* rowImpl = 
        dynamic_cast<BatchWriteRequestPutRowItemImpl*>(rowItem);
    pbRow->Swap(dynamic_cast<cloudservice::ots2::PutRowInBatchWriteRowRequest*>(rowImpl->GetPBMessage()));
    delete rowItem;
}

void BatchWriteRequestTableItemImpl::AddUpdateRowItem(BatchWriteRequestUpdateRowItem* rowItem)
{
    if (NULL == rowItem) {
        OTS_THROW("OTSClient", "ClientParameterError", "BatchWriteRequestUpdateRowItem is null");
    }
    cloudservice::ots2::UpdateRowInBatchWriteRowRequest* pbRow = mPBMessage->add_update_rows();
    BatchWriteRequestUpdateRowItemImpl* rowImpl = 
        dynamic_cast<BatchWriteRequestUpdateRowItemImpl*>(rowItem);
    pbRow->Swap(dynamic_cast<cloudservice::ots2::UpdateRowInBatchWriteRowRequest*>(rowImpl->GetPBMessage()));
    delete rowItem;
}

void BatchWriteRequestTableItemImpl::AddDeleteRowItem(BatchWriteRequestDeleteRowItem* rowItem)
{
    if (NULL == rowItem) {
        OTS_THROW("OTSClient", "ClientParameterError", "BatchWriteRequestDeleteRowItem is null");
    }
    cloudservice::ots2::DeleteRowInBatchWriteRowRequest* pbRow = mPBMessage->add_delete_rows();
    BatchWriteRequestDeleteRowItemImpl* rowImpl = 
        dynamic_cast<BatchWriteRequestDeleteRowItemImpl*>(rowItem);
    pbRow->Swap(dynamic_cast<cloudservice::ots2::DeleteRowInBatchWriteRowRequest*>(rowImpl->GetPBMessage()));
    delete rowItem;
}

Message* BatchWriteRequestTableItemImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchWriteRowRequestImpl
BatchWriteRowRequestImpl::BatchWriteRowRequestImpl(
            cloudservice::ots2::BatchWriteRowRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchWriteRowRequestImpl::~BatchWriteRowRequestImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void BatchWriteRowRequestImpl::AddTableItem(BatchWriteRequestTableItem* tableItem)
{
    if (NULL == tableItem) {
        OTS_THROW("OTSClient", "ClientParameterError", "BatchWriteRequestTableItem is null");
    }
    cloudservice::ots2::TableInBatchWriteRowRequest* pbTable = mPBMessage->add_tables();
    BatchWriteRequestTableItemImpl* tableImpl = 
        dynamic_cast<BatchWriteRequestTableItemImpl*>(tableItem);
    pbTable->Swap(dynamic_cast<cloudservice::ots2::TableInBatchWriteRowRequest*>(tableImpl->GetPBMessage()));
    delete tableItem;
}

Message* BatchWriteRowRequestImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchWriteResponseRowItemImpl
BatchWriteResponseRowItemImpl::BatchWriteResponseRowItemImpl(
            cloudservice::ots2::RowInBatchWriteRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message),
    mError(NULL), mConsumedCapacity(NULL)
{
}

BatchWriteResponseRowItemImpl::~BatchWriteResponseRowItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mError;
    delete mConsumedCapacity;
}

bool BatchWriteResponseRowItemImpl::IsOK() const
{
    return mPBMessage->is_ok();
}

const Error& BatchWriteResponseRowItemImpl::GetError() const
{
    if (mPBMessage->is_ok()) {
        OTS_THROW("OTSClient", "ClientParameterError", "The row's result is ok");
    }
    if (NULL == mError) {
        OTS_THROW("OTSClient", "ClientInternalError", "Error is null");
    }
    return *mError;
}

const CapacityUnit& BatchWriteResponseRowItemImpl::GetConsumedCapacity() const
{
    if (!mPBMessage->is_ok()) {
        OTS_THROW("OTSClient", "ClientParameterError", "The row's result is not ok");
    }
    if (NULL == mConsumedCapacity) {
        OTS_THROW("OTSClient", "ClientInternalError", "CapacityUnit is null");
    }
    return *mConsumedCapacity;
}

void BatchWriteResponseRowItemImpl::ParseFromPBMessage()
{
    if (mPBMessage->is_ok()) {
        cloudservice::ots2::ConsumedCapacity* pbConsumed = mPBMessage->mutable_consumed();
        cloudservice::ots2::CapacityUnit* pbCapacity = pbConsumed->mutable_capacity_unit();
        mConsumedCapacity = OTSFactoryImpl::NewCapacityUnit(pbCapacity);
    } else {
        cloudservice::ots2::Error* pbError = mPBMessage->mutable_error();
        mError = OTSFactoryImpl::NewError(pbError);
    }
}

Message* BatchWriteResponseRowItemImpl::GetPBMessage() const
{
    return mPBMessage;
}


//BatchWriteResponseTableItemImpl
BatchWriteResponseTableItemImpl::BatchWriteResponseTableItemImpl(
            cloudservice::ots2::TableInBatchWriteRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchWriteResponseTableItemImpl::~BatchWriteResponseTableItemImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    for (int32_t i = 0; i < (int32_t)mPutRowItems.size(); ++i) {
        delete mPutRowItems[i];
    }
    for (int32_t i = 0; i < (int32_t)mUpdateRowItems.size(); ++i) {
        delete mUpdateRowItems[i];
    }
    for (int32_t i = 0; i < (int32_t)mDeleteRowItems.size(); ++i) {
        delete mDeleteRowItems[i];
    }
}

const string& BatchWriteResponseTableItemImpl::GetTableName() const
{
    return mPBMessage->table_name();
}

int32_t BatchWriteResponseTableItemImpl::GetPutRowItemSize() const
{
    return mPutRowItems.size();
}

const BatchWriteResponseRowItem& BatchWriteResponseTableItemImpl::GetPutRowItem(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mPutRowItems.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mPutRowItems[index];
}

int32_t BatchWriteResponseTableItemImpl::GetUpdateRowItemSize() const
{
    return mUpdateRowItems.size();
}

const BatchWriteResponseRowItem& BatchWriteResponseTableItemImpl::GetUpdateRowItem(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mUpdateRowItems.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mUpdateRowItems[index];
}

int32_t BatchWriteResponseTableItemImpl::GetDeleteRowItemSize() const
{
    return mDeleteRowItems.size();
}

const BatchWriteResponseRowItem& BatchWriteResponseTableItemImpl::GetDeleteRowItem(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mDeleteRowItems.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mDeleteRowItems[index];
}

void BatchWriteResponseTableItemImpl::ParseFromPBMessage()
{
    // PutRow
    int32_t putRowSize = mPBMessage->put_rows_size();
    for (int32_t i = 0; i < putRowSize; ++i) {
        cloudservice::ots2::RowInBatchWriteRowResponse* pbRow = mPBMessage->mutable_put_rows(i);
        BatchWriteResponseRowItem* row = OTSFactoryImpl::NewBatchWriteResponseRowItem(pbRow);
        BatchWriteResponseRowItemImpl* rowImpl = dynamic_cast<BatchWriteResponseRowItemImpl*>(row);
        rowImpl->ParseFromPBMessage();
        mPutRowItems.push_back(row);
    }

    // UpdateRow
    int32_t updateRowSize = mPBMessage->update_rows_size();
    for (int32_t i = 0; i < updateRowSize; ++i) {
        cloudservice::ots2::RowInBatchWriteRowResponse* pbRow = mPBMessage->mutable_update_rows(i);
        BatchWriteResponseRowItem* row = OTSFactoryImpl::NewBatchWriteResponseRowItem(pbRow);
        BatchWriteResponseRowItemImpl* rowImpl = dynamic_cast<BatchWriteResponseRowItemImpl*>(row);
        rowImpl->ParseFromPBMessage();
        mUpdateRowItems.push_back(row);
    }

    // PutRow
    int32_t deleteRowSize = mPBMessage->delete_rows_size();
    for (int32_t i = 0; i < deleteRowSize; ++i) {
        cloudservice::ots2::RowInBatchWriteRowResponse* pbRow = mPBMessage->mutable_delete_rows(i);
        BatchWriteResponseRowItem* row = OTSFactoryImpl::NewBatchWriteResponseRowItem(pbRow);
        BatchWriteResponseRowItemImpl* rowImpl = dynamic_cast<BatchWriteResponseRowItemImpl*>(row);
        rowImpl->ParseFromPBMessage();
        mDeleteRowItems.push_back(row);
    }
}

Message* BatchWriteResponseTableItemImpl::GetPBMessage() const
{
    return mPBMessage;
}
    


//BatchWriteRowResponseImpl
BatchWriteRowResponseImpl::BatchWriteRowResponseImpl(
            cloudservice::ots2::BatchWriteRowResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

BatchWriteRowResponseImpl::~BatchWriteRowResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    for (int32_t i = 0; i < (int32_t)mTableItems.size(); ++i) {
        delete mTableItems[i];
    }
}

int32_t BatchWriteRowResponseImpl::GetTableItemSize() const
{
    return mTableItems.size();
}

const BatchWriteResponseTableItem& BatchWriteRowResponseImpl::GetTableItem(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mTableItems.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mTableItems[index];
}

void BatchWriteRowResponseImpl::ParseFromPBMessage()
{
    int32_t tableSize = mPBMessage->tables_size();
    for (int32_t i = 0; i < tableSize; ++i) {
        cloudservice::ots2::TableInBatchWriteRowResponse* pbTable = mPBMessage->mutable_tables(i);
        BatchWriteResponseTableItem* table = OTSFactoryImpl::NewBatchWriteResponseTableItem(pbTable);
        BatchWriteResponseTableItemImpl* tableImpl = dynamic_cast<BatchWriteResponseTableItemImpl*>(table);
        tableImpl->ParseFromPBMessage();
        mTableItems.push_back(table);
    }
}

Message* BatchWriteRowResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}


//GetRangeRequestImpl
GetRangeRequestImpl::GetRangeRequestImpl(
            cloudservice::ots2::GetRangeRequest* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message)
{
}

GetRangeRequestImpl::~GetRangeRequestImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
}

void GetRangeRequestImpl::SetTableName(const string& tableName)
{
    mPBMessage->set_table_name(tableName);
}

void GetRangeRequestImpl::SetDirection(RangeDirection direction)
{
    switch (direction)
    {
        case FORWARD:
            mPBMessage->set_direction(cloudservice::ots2::FORWARD);
            break;
        case BACKWARD:
            mPBMessage->set_direction(cloudservice::ots2::BACKWARD);
            break;
    }
}

void GetRangeRequestImpl::SetLimit(int32_t limit)
{
    mPBMessage->set_limit(limit);
}

void GetRangeRequestImpl::AddStartPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_inclusive_start_primary_key(); 
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

void GetRangeRequestImpl::AddEndPrimaryKey(Column* column)
{
    if (NULL == column) {
        OTS_THROW("OTSClient", "ClientParameterError", "Primary key is null");
    }
    cloudservice::ots2::Column* pbColumn = mPBMessage->add_exclusive_end_primary_key(); 
    ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
    pbColumn->Swap(dynamic_cast<cloudservice::ots2::Column*>(columnImpl->GetPBMessage()));
    delete column;
}

void GetRangeRequestImpl::AddColumnName(const string& name)
{
    mPBMessage->add_columns_to_get(name);
}

Message* GetRangeRequestImpl::GetPBMessage() const
{
    return mPBMessage;
}


//GetRangeResponseImpl
GetRangeResponseImpl::GetRangeResponseImpl(
            cloudservice::ots2::GetRangeResponse* message,
            Ownership ownership)
: mOwnership(ownership), mPBMessage(message), mConsumedCapacity(NULL)
{
}

GetRangeResponseImpl::~GetRangeResponseImpl()
{
    if (OWN == mOwnership) {
        delete mPBMessage;
    }
    delete mConsumedCapacity;
    for (int32_t i = 0; i < (int32_t)mNextStartPrimaryKeys.size(); ++i) {
        delete mNextStartPrimaryKeys[i];
    }
    for (int32_t i = 0; i < (int32_t)mRows.size(); ++i) {
        delete mRows[i];
    }
}

const CapacityUnit& GetRangeResponseImpl::GetConsumedCapacity() const
{
    return *mConsumedCapacity;
}

int32_t GetRangeResponseImpl::GetNextStartPrimaryKeySize() const
{
    return mNextStartPrimaryKeys.size();
}

const Column& GetRangeResponseImpl::GetNextStartPrimaryKey(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mNextStartPrimaryKeys.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mNextStartPrimaryKeys[index];
}

int32_t GetRangeResponseImpl::GetRowSize() const
{
    return mRows.size();
}

const Row& GetRangeResponseImpl::GetRow(int32_t index) const
{
    if (index < 0 || index >= (int32_t)mRows.size()) {
        OTS_THROW("OTSClient", "ClientParameterError", "Index is out of range");
    }
    return *mRows[index];
}

void GetRangeResponseImpl::ParseFromPBMessage()
{
    // Parse CapacityUnit
    cloudservice::ots2::ConsumedCapacity* pbConsumed = mPBMessage->mutable_consumed();
    cloudservice::ots2::CapacityUnit* pbCapacity = pbConsumed->mutable_capacity_unit();
    mConsumedCapacity = OTSFactoryImpl::NewCapacityUnit(pbCapacity);

    // Parse NextStartPrimaryKey
    int32_t pkSize = mPBMessage->next_start_primary_key_size();
    for (int32_t i = 0; i < pkSize; ++i) {
        cloudservice::ots2::Column* pbColumn = mPBMessage->mutable_next_start_primary_key(i);
        Column* column = OTSFactoryImpl::NewColumn(pbColumn);
        ColumnImpl* columnImpl = dynamic_cast<ColumnImpl*>(column);
        columnImpl->ParseFromPBMessage();
        mNextStartPrimaryKeys.push_back(column);
    }

    // Parse Row
    int32_t rowSize = mPBMessage->rows_size();
    for (int32_t i = 0; i < rowSize; ++i) {
        cloudservice::ots2::Row* pbRow = mPBMessage->mutable_rows(i);
        Row* row = OTSFactoryImpl::NewRow(pbRow);
        RowImpl* rowImpl = dynamic_cast<RowImpl*>(row); 
        rowImpl->ParseFromPBMessage();
        mRows.push_back(row);
    }
}

Message* GetRangeResponseImpl::GetPBMessage() const
{
    return mPBMessage;
}


//RowIteratorImpl
RowIteratorImpl::RowIteratorImpl(
            cloudservice::ots2::GetRangeRequest* pbRequest,
            cloudservice::ots2::GetRangeResponse* pbResponse,
            Ownership ownership)
: mOwnership(ownership), mPBRequest(pbRequest), mPBResponse(pbResponse),
    mClientImpl(NULL), mConsumedReadCapacity(NULL), mRowCount(-1), mRowIndex(0)
{
}

void RowIteratorImpl::InitForFirstTime(
            OTSClientImpl* clientImpl,
            cloudservice::ots2::GetRangeRequest* pbRequest,
            int64_t* consumed_read_capacity,
            int32_t row_count)
{
    mClientImpl = clientImpl;
    mPBRequest->CopyFrom(*pbRequest);
    mConsumedReadCapacity = consumed_read_capacity;
    if (row_count > 0) {
        mRowCount = row_count;
    }

    cloudservice::ots2::ConsumedCapacity* pbConsumed = mPBResponse->mutable_consumed();
    cloudservice::ots2::CapacityUnit* pbCapacity = pbConsumed->mutable_capacity_unit();
    if (pbCapacity->has_read()) {
        *mConsumedReadCapacity += pbCapacity->read();
    }
}

Message* RowIteratorImpl::GetPBMessage() const
{
    return mPBResponse;
}

RowIteratorImpl::~RowIteratorImpl()
{
    if (OWN == mOwnership) {
        delete mPBRequest;
        delete mPBResponse;
    }
    for (int32_t i = 0; i < (int32_t)mRows.size(); ++i) {
        delete mRows[i];
    }
}

bool RowIteratorImpl::HasNext() const
{
    return (mPBResponse != NULL && mRowIndex < mPBResponse->rows_size()) ? true : false;
}

const Row& RowIteratorImpl::Next()
{
    if (!HasNext()) {
        OTS_THROW("OTSClient", "ClientParameterError", "no more rows");
    }

    cloudservice::ots2::Row* pbRow = mPBResponse->mutable_rows(mRowIndex); 
    Row* row = OTSFactoryImpl::NewRow(pbRow);
    RowImpl* rowImpl = dynamic_cast<RowImpl*>(row);
    rowImpl->ParseFromPBMessage();
    mRows.push_back(row);

    // Try to perform more GetRange().
    if (++mRowIndex >= mPBResponse->rows_size() && 
            mPBResponse->next_start_primary_key_size() > 0 && 
            (mRowCount < 0 || mRowCount - mPBResponse->rows_size() > 0)) {
        // Copy next_start_primary_keys to inclusive_start_primary_keys
        mPBRequest->clear_inclusive_start_primary_key(); 
        for (int i = 0; i < mPBResponse->next_start_primary_key_size(); ++i) {
            cloudservice::ots2::Column* pbColumn = mPBRequest->add_inclusive_start_primary_key(); 
            pbColumn->CopyFrom(mPBResponse->next_start_primary_key(i));
        }
        // Reset left limit.
        if (mRowCount > 0) {
            mRowCount -= mPBResponse->rows_size();
            mPBRequest->set_limit(mRowCount);
        }

        // Call GetRange again and update consumed capacity.
        mClientImpl->GetNextRange(mPBRequest, mPBResponse);
        cloudservice::ots2::ConsumedCapacity* pbConsumed = mPBResponse->mutable_consumed();
        cloudservice::ots2::CapacityUnit* pbCapacity = pbConsumed->mutable_capacity_unit();
        if (pbCapacity->has_read()) {
            *mConsumedReadCapacity += pbCapacity->read();
        }
        mRowIndex = 0;
    }
    return *row;
}

} //end of ots
} //end of openservices
} //end of aliyun
