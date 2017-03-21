/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * 版权所有 （C）阿里云计算有限公司
 */

#include "ots_row_iterator.h"
#include "ots_client_impl.h"

namespace aliyun {
namespace tablestore {

OTSRowIterator::OTSRowIterator(
    const GetRangeRequestPtr& requestPtr,
    void* clientImpl)
    : mClientImpl(clientImpl)
    , mHasNextPK(true)
{
    mRequestPtr.reset(new GetRangeRequest(requestPtr->GetRowQueryCriteria()));
}

OTSRowIterator::~OTSRowIterator()
{
}

bool OTSRowIterator::HasNext()
{
    if ((mResultPtr.get() == NULL || mRowIndex >= mRows.size()) && mHasNextPK) {
        mRowIndex = 0;
        mRows.clear();
        do {
            mResultPtr = ((OTSClientImpl*)mClientImpl)->GetRange(mRequestPtr);
            const std::list<RowPtr>& rowPtrs = mResultPtr->GetRows();
            mRows.reserve(rowPtrs.size());
            typeof(rowPtrs.begin()) iter = rowPtrs.begin();
            for (; iter != rowPtrs.end(); ++iter) {
                mRows.push_back(*iter); 
            }
            if (mResultPtr->HasNextStartPrimaryKey()) {
                mRequestPtr->SetInclusiveStartPrimaryKey(mResultPtr->GetNextStartPrimaryKey());
            } else {
                mHasNextPK = false;
                break;
            }
        } while (mRows.empty());
    }
    return (mRowIndex < mRows.size()) ? true : false;
}

RowPtr OTSRowIterator::Next()
{
    if (!HasNext()) {
        throw OTSClientException("No more row.");
    }
    return mRows[mRowIndex++];
}

} // end of tablestore
} // end of aliyun
