/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * 版权所有 （C）阿里云计算有限公司
 */
#ifndef OTS_ROW_ITERATOR_H
#define OTS_ROW_ITERATOR_H

#include "ots/ots_common.h"
#include "ots/ots_types.h"
#include "ots/ots_request.h"
#include <vector>

namespace aliyun {
namespace tablestore {

class OTSRowIterator : public Iterator<RowPtr>
{
public:

    OTSRowIterator(
        const GetRangeRequestPtr& requestPtr,
        void* asyncClientImpl);

    virtual ~OTSRowIterator();

    virtual bool HasNext();

    virtual RowPtr Next();

private:

    GetRangeRequestPtr mRequestPtr;
    GetRangeResultPtr mResultPtr;
    void* mClientImpl;

    bool mHasNextPK;
    size_t mRowIndex;
    std::vector<RowPtr> mRows; 
};

} // end of tablestore
} // end of aliyun

#endif
