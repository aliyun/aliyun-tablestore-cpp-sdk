#include "include/ots_client.h"
#include "ots_client_impl.h"
#include "ots_types_impl.h"

using namespace std;
using namespace com::aliyun;
using namespace aliyun::openservices::ots;

// OTSConfig
OTSConfig::OTSConfig()
    : mRequestTimeout(30), mMaxErrorRetry(3), 
      mRequestCompressType(COMPRESS_NO),
      mResponseCompressType(COMPRESS_NO)
{
}


// OTSClient
OTSClient::OTSClient(const OTSConfig& otsConfig)
{
    mClientImpl = (void*)(new OTSClientImpl(otsConfig));
}

OTSClient::~OTSClient()
{
    delete (OTSClientImpl*)mClientImpl;
}

void OTSClient::CreateTable(const CreateTableRequest& request)
{
    ((OTSClientImpl*)mClientImpl)->CreateTable(request); 
}

void OTSClient::DescribeTable(
            const std::string& tableName,
            DescribeTableResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->DescribeTable(tableName, response);
}

void OTSClient::UpdateTable(
            const UpdateTableRequest& request,
            UpdateTableResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->UpdateTable(request, response);
}

void OTSClient::ListTable(ListTableResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->ListTable(response);
}

void OTSClient::DeleteTable(const std::string& tableName)
{
    ((OTSClientImpl*)mClientImpl)->DeleteTable(tableName);
}

void OTSClient::GetRow(
            const GetRowRequest& request,
            GetRowResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->GetRow(request, response);
}

void OTSClient::PutRow(
            const PutRowRequest& request,
            PutRowResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->PutRow(request, response);
}

void OTSClient::UpdateRow(
            const UpdateRowRequest& request,
            UpdateRowResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->UpdateRow(request, response);
}

void OTSClient::DeleteRow(
            const DeleteRowRequest& request,
            DeleteRowResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->DeleteRow(request, response);
}

void OTSClient::BatchGetRow(
            const BatchGetRowRequest& request,
            BatchGetRowResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->BatchGetRow(request, response);
}

void OTSClient::BatchWriteRow(
            const BatchWriteRowRequest& request,
            BatchWriteRowResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->BatchWriteRow(request, response);
}

void OTSClient::GetRange(
            const GetRangeRequest& request,
            GetRangeResponse* response)
{
    ((OTSClientImpl*)mClientImpl)->GetRange(request, response);
}

void OTSClient::GetRangeByIterator(
            const GetRangeRequest& request, 
            int64_t* consumed_capacity_unit, 
            RowIterator* iterator,
            int32_t row_count)
{
    ((OTSClientImpl*)mClientImpl)->GetRangeByIterator(
                request, consumed_capacity_unit, iterator, row_count);
}
