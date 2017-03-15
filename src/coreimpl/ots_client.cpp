/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#include "ots/ots_client.h"
#include "ots_client_impl.h"

using namespace std;

namespace aliyun {
namespace tablestore {

OTSClient::OTSClient(
    const string& endpoint,
    const string& accessKeyId,
    const string& accessKeySecret,
    const string& instanceName)
{
    Credential auth(accessKeyId, accessKeySecret);
    ClientConfiguration cc;
    mClientImpl = (void*)(new OTSClientImpl(
            endpoint, instanceName, auth, cc));
}

OTSClient::OTSClient(
    const string& endpoint,
    const string& accessKeyId,
    const string& accessKeySecret,
    const string& instanceName,
    const ClientConfiguration& clientConfig)
{
    Credential auth(accessKeyId, accessKeySecret);
    mClientImpl = (void*)(new OTSClientImpl(
            endpoint, instanceName, auth, clientConfig));
}

OTSClient::OTSClient(
    const string& endpoint,
    const string& instanceName,
    const Credential& auth,
    const ClientConfiguration& clientConfig)
{
    mClientImpl = (void*)(new OTSClientImpl(
            endpoint, instanceName, auth, clientConfig));
}

OTSClient::~OTSClient()
{
    delete (OTSClientImpl*)mClientImpl;
}

CreateTableResultPtr OTSClient::CreateTable(const CreateTableRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->CreateTable(requestPtr);
}

ListTableResultPtr OTSClient::ListTable()
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->ListTable();
}

DescribeTableResultPtr OTSClient::DescribeTable(const DescribeTableRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->DescribeTable(requestPtr);
}

DeleteTableResultPtr OTSClient::DeleteTable(const DeleteTableRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->DeleteTable(requestPtr);
}

UpdateTableResultPtr OTSClient::UpdateTable(const UpdateTableRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->UpdateTable(requestPtr);
}

GetRowResultPtr OTSClient::GetRow(const GetRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->GetRow(requestPtr);
}

PutRowResultPtr OTSClient::PutRow(const PutRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->PutRow(requestPtr);
}

UpdateRowResultPtr OTSClient::UpdateRow(const UpdateRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->UpdateRow(requestPtr);
}

DeleteRowResultPtr OTSClient::DeleteRow(const DeleteRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->DeleteRow(requestPtr);
}

BatchGetRowResultPtr OTSClient::BatchGetRow(const BatchGetRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->BatchGetRow(requestPtr);
}

BatchWriteRowResultPtr OTSClient::BatchWriteRow(const BatchWriteRowRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->BatchWriteRow(requestPtr);
}

GetRangeResultPtr OTSClient::GetRange(const GetRangeRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->GetRange(requestPtr);
}

RowIteratorPtr OTSClient::GetRangeIterator(const GetRangeRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->GetRangeIterator(requestPtr);
}

ComputeSplitsBySizeResultPtr OTSClient::ComputeSplitsBySize(const ComputeSplitsBySizeRequestPtr& requestPtr)
    throw (OTSException, OTSClientException)
{
    return ((OTSClientImpl*)mClientImpl)->ComputeSplitsBySize(requestPtr);
}

} // end of tablestore
} // end of aliyun
