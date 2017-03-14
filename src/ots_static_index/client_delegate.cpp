#include "ots_static_index/client_delegate.h"
#include "jsonize.h"
#include "type_delegates.h"
#include "logging_assert.h"
#include "foreach.h"
#include "ots_static_index/logger.h"
#include <tr1/tuple>
#include <memory>
#include <map>

using namespace ::std;
using namespace ::std::tr1;

namespace static_index {

namespace {

class ClientDelegateImpl: public ClientDelegate
{
    Logger* mLogger;
    aliyun::openservices::ots::OTSConfig mConfig;

public:
    explicit ClientDelegateImpl(
        Logger* logger,
        const aliyun::openservices::ots::OTSConfig& cfg)
      : mLogger(logger),
        mConfig(cfg)
    {
        OTS_LOG_INFO(mLogger)(cfg.mEndPoint)(cfg.mInstanceName);
    }

    Exceptional PutRow(PutRowResponse* res, const PutRowRequest& req);
    Exceptional BatchWrite(BatchWriteResponse* resp, const BatchWriteRequest& req);
    Exceptional GetRange(GetRangeResponse* res, const GetRangeRequest& req);
    Exceptional BatchGetRow(BatchGetRowResponse*, const BatchGetRowRequest&);

private:
    aliyun::openservices::ots::OTSValue* From(const Value& val);
    aliyun::openservices::ots::Column* From(const Column& col);
    aliyun::openservices::ots::Condition* From(const Condition& cond);
    aliyun::openservices::ots::PutRowRequest* From(const PutRowRequest& req);
    aliyun::openservices::ots::BatchWriteRowRequest* From(const BatchWriteRequest& req);
    aliyun::openservices::ots::GetRangeRequest* From(const GetRangeRequest& req);
    aliyun::openservices::ots::BatchGetRowRequest* From(const BatchGetRowRequest& req);

    void Into(
        BatchWriteResponse* resp,
        const aliyun::openservices::ots::BatchWriteRowResponse& ores,
        const BatchWriteRequest& req);
    void Into(
        BatchGetRowResponse* resp,
        const aliyun::openservices::ots::BatchGetRowResponse& ores,
        const BatchGetRowRequest& req);
    void Into(
        GetRangeResponse* resp,
        const aliyun::openservices::ots::GetRangeResponse& ores);
    void Into(
        Row* out,
        const aliyun::openservices::ots::Row& in);
    void Into(
        Column* out,
        const aliyun::openservices::ots::Column& in);
    void Into(
        Value* out,
        const aliyun::openservices::ots::OTSValue& in);
};

Exceptional ClientDelegateImpl::PutRow(
    PutRowResponse* res,
    const PutRowRequest& req)
{
    OTS_LOG_INFO(mLogger)(req.mTracker)(req.mTableName);
    OTS_LOG_DEBUG(mLogger)(ToString(Jsonize(mLogger, req)));
    aliyun::openservices::ots::OTSFactory fac;
    auto_ptr<aliyun::openservices::ots::PutRowRequest> oreq(From(req));
    auto_ptr<aliyun::openservices::ots::PutRowResponse> ores(
        fac.NewPutRowResponse());

    try {
        auto_ptr<aliyun::openservices::ots::OTSClient> client(
            new aliyun::openservices::ots::OTSClient(mConfig));
        client->PutRow(*oreq, ores.get());
        return Exceptional();
    }
    catch(const aliyun::openservices::ots::OTSException& ex) {
        return Exceptional(ex);
    }
}


Exceptional ClientDelegateImpl::BatchWrite(
    BatchWriteResponse* resp,
    const BatchWriteRequest& req)
{
    OTS_LOG_INFO(mLogger)(req.mTracker).What("enter BatchWrite");
    OTS_LOG_DEBUG(mLogger)(ToString(Jsonize(mLogger, req)));
    aliyun::openservices::ots::OTSFactory fac;
    auto_ptr<aliyun::openservices::ots::BatchWriteRowRequest> oreq(
        From(req));
    auto_ptr<aliyun::openservices::ots::BatchWriteRowResponse> ores(
        fac.NewBatchWriteRowResponse());

    try {
        auto_ptr<aliyun::openservices::ots::OTSClient> client(
            new aliyun::openservices::ots::OTSClient(mConfig));
        client->BatchWriteRow(*oreq, ores.get());
        Into(resp, *ores, req);
        OTS_LOG_INFO(mLogger)(req.mTracker).What("BatchWrite succeed");
        OTS_LOG_DEBUG(mLogger)
            (ToString(Jsonize(mLogger, req)))
            (ToString(Jsonize(mLogger, *resp)));
        return Exceptional();
    }
    catch(const aliyun::openservices::ots::OTSException& ex) {
        OTS_LOG_ERROR(mLogger)
            (ex.GetRequestId())
            (ex.GetOperation())
            (ex.GetErrorCode())
            (ex.GetErrorMessage());
        return Exceptional(ex);
    }
}

Exceptional ClientDelegateImpl::GetRange(
    GetRangeResponse* resp,
    const GetRangeRequest& req)
{
    aliyun::openservices::ots::OTSFactory fac;
    auto_ptr<aliyun::openservices::ots::GetRangeRequest> oreq(
        From(req));
    auto_ptr<aliyun::openservices::ots::GetRangeResponse> ores(
        fac.NewGetRangeResponse());

    try {
        auto_ptr<aliyun::openservices::ots::OTSClient> client(
            new aliyun::openservices::ots::OTSClient(mConfig));
        client->GetRange(*oreq, ores.get());
        Into(resp, *ores);
        return Exceptional();
    }
    catch(const aliyun::openservices::ots::OTSException& ex) {
        return Exceptional(ex);
    }
}

Exceptional ClientDelegateImpl::BatchGetRow(
    BatchGetRowResponse* resp,
    const BatchGetRowRequest& req)
{
    aliyun::openservices::ots::OTSFactory fac;
    auto_ptr<aliyun::openservices::ots::BatchGetRowRequest> oreq(
        From(req));
    auto_ptr<aliyun::openservices::ots::BatchGetRowResponse> ores(
        fac.NewBatchGetRowResponse());

    try {
        auto_ptr<aliyun::openservices::ots::OTSClient> client(
            new aliyun::openservices::ots::OTSClient(mConfig));
        client->BatchGetRow(*oreq, ores.get());
        Into(resp, *ores, req);
        return Exceptional();
    }
    catch(const aliyun::openservices::ots::OTSException& ex) {
        return Exceptional(ex);
    }
}


aliyun::openservices::ots::OTSValue* ClientDelegateImpl::From(const Value& val)
{
    aliyun::openservices::ots::OTSFactory fac;
    switch(val.GetType()) {
    case Value::INF_MIN: return fac.NewInfMinValue();
    case Value::INF_MAX: return fac.NewInfMaxValue();
    case Value::INTEGER: return fac.NewIntValue(val.Int());
    case Value::STRING: return fac.NewStringValue(val.Str());
    case Value::BOOLEAN: return fac.NewBoolValue(val.Bool());
    case Value::DOUBLE: return fac.NewDoubleValue(val.Double());
    default: {
        OTS_ASSERT(mLogger, false)((int) val.GetType()).What("unsupported value type");
        return NULL;
    }
    }
}

aliyun::openservices::ots::Column* ClientDelegateImpl::From(const Column& col)
{
    aliyun::openservices::ots::OTSFactory fac;
    return fac.NewColumn(col.mName, From(col.mValue));
}

aliyun::openservices::ots::Condition* ClientDelegateImpl::From(const Condition& cond)
{
    aliyun::openservices::ots::OTSFactory fac;
    return fac.NewCondition(cond.mRowExist);
}

aliyun::openservices::ots::PutRowRequest* ClientDelegateImpl::From(
    const PutRowRequest& req)
{
    aliyun::openservices::ots::OTSFactory fac;
    auto_ptr<aliyun::openservices::ots::PutRowRequest> oreq(
        fac.NewPutRowRequest());
    oreq->SetTableName(req.mTableName);
    oreq->SetCondition(From(req.mCondition));
    FOREACH_ITER(i, req.mPrimaryKey) {
        const Column& c = *i;
        oreq->AddPrimaryKey(From(c));
    }
    FOREACH_ITER(i, req.mAttributes) {
        const Column& c = *i;
        oreq->AddColumn(From(c));
    }
    return oreq.release();
}

aliyun::openservices::ots::BatchWriteRowRequest* ClientDelegateImpl::From(
    const BatchWriteRequest& req)
{
    typedef map<string, deque<const PutRowRequest*> > PutAggregator;
    PutAggregator puts;
    FOREACH_ITER(i, req.mPutRows) {
        const PutRowRequest& put = get<0>(*i);
        puts[put.mTableName].push_back(&put);
    }
    typedef map<string, deque<const DeleteRowRequest*> > DelAggregator;
    DelAggregator dels;
    FOREACH_ITER(i, req.mDelRows) {
        const DeleteRowRequest& del = get<0>(*i);
        dels[del.mTableName].push_back(&del);
    }

    aliyun::openservices::ots::OTSFactory fac;
    auto_ptr<aliyun::openservices::ots::BatchWriteRowRequest> res(
        fac.NewBatchWriteRowRequest());
    FOREACH_ITER(i, puts) {
        const string& tbl = i->first;
        const deque<const PutRowRequest*>& rows = i->second;
        auto_ptr<aliyun::openservices::ots::BatchWriteRequestTableItem> tblItem(
            fac.NewBatchWriteRequestTableItem());
        tblItem->SetTableName(tbl);
        FOREACH_ITER(j, rows) {
            const PutRowRequest& put = **j;
            auto_ptr<aliyun::openservices::ots::BatchWriteRequestPutRowItem> putItem(
                fac.NewBatchWriteRequestPutRowItem());
            putItem->SetCondition(From(put.mCondition));
            FOREACH_ITER(k, put.mPrimaryKey) {
                const Column& c = *k;
                putItem->AddPrimaryKey(From(c));
            }
            FOREACH_ITER(k, put.mAttributes) {
                const Column& c = *k;
                putItem->AddColumn(From(c));
            }
            tblItem->AddPutRowItem(putItem.release());
        }
        res->AddTableItem(tblItem.release());
    }
    FOREACH_ITER(i, dels) {
        const string& tbl = i->first;
        const deque<const DeleteRowRequest*>& rows = i->second;
        auto_ptr<aliyun::openservices::ots::BatchWriteRequestTableItem> tblItem(
            fac.NewBatchWriteRequestTableItem());
        tblItem->SetTableName(tbl);
        FOREACH_ITER(j, rows) {
            const DeleteRowRequest& del = **j;
            auto_ptr<aliyun::openservices::ots::BatchWriteRequestDeleteRowItem> delItem(
                fac.NewBatchWriteRequestDeleteRowItem());
            delItem->SetCondition(From(del.mCondition));
            FOREACH_ITER(k, del.mPrimaryKey) {
                const Column& c = *k;
                delItem->AddPrimaryKey(From(c));
            }
            tblItem->AddDeleteRowItem(delItem.release());
        }
        res->AddTableItem(tblItem.release());
    }

    return res.release();
}

aliyun::openservices::ots::GetRangeRequest* ClientDelegateImpl::From(
    const GetRangeRequest& req)
{
    aliyun::openservices::ots::OTSFactory fac;
    auto_ptr<aliyun::openservices::ots::GetRangeRequest> res(
        fac.NewGetRangeRequest());
    res->SetTableName(req.mTableName);
    res->SetDirection(aliyun::openservices::ots::FORWARD);
    res->SetLimit(req.mLimit);
    FOREACH_ITER(i, req.mStartPkey) {
        res->AddStartPrimaryKey(From(*i));
    }
    FOREACH_ITER(i, req.mStopPkey) {
        res->AddEndPrimaryKey(From(*i));
    }

    return res.release();
}

aliyun::openservices::ots::BatchGetRowRequest* ClientDelegateImpl::From(
    const BatchGetRowRequest& req)
{
    typedef map<string, deque<const GetRowRequest*> > GetAggregator;
    GetAggregator gets;
    FOREACH_ITER(i, req.mGetRows) {
        const GetRowRequest& g = get<0>(*i);
        gets[g.mTableName].push_back(&g);
    }

    aliyun::openservices::ots::OTSFactory fac;
    auto_ptr<aliyun::openservices::ots::BatchGetRowRequest> res(
        fac.NewBatchGetRowRequest());
    FOREACH_ITER(i, gets) {
        const string& tbl = i->first;
        const deque<const GetRowRequest*>& rows = i->second;
        auto_ptr<aliyun::openservices::ots::BatchGetRequestTableItem> tblItem(
            fac.NewBatchGetRequestTableItem());
        tblItem->SetTableName(tbl);
        FOREACH_ITER(j, rows) {
            const GetRowRequest& put = **j;
            auto_ptr<aliyun::openservices::ots::BatchGetRequestRowItem> rowItem(
                fac.NewBatchGetRequestRowItem());
            FOREACH_ITER(k, put.mPrimaryKey) {
                const Column& c = *k;
                rowItem->AddPrimaryKey(From(c));
            }
            tblItem->AddRowItem(rowItem.release());
        }
        res->AddTableItem(tblItem.release());
    }

    return res.release();
}


void ClientDelegateImpl::Into(
    BatchWriteResponse* resp,
    const aliyun::openservices::ots::BatchWriteRowResponse& ores,
    const BatchWriteRequest& req)
{
    typedef map<string, deque<const void*> > PutAggregator;
    PutAggregator puts;
    FOREACH_ITER(i, req.mPutRows) {
        const PutRowRequest& put = get<0>(*i);
        const void* user = get<1>(*i);
        puts[put.mTableName].push_back(user);
    }
    typedef map<string, deque<const void*> > DelAggregator;
    DelAggregator dels;
    FOREACH_ITER(i, req.mDelRows) {
        const DeleteRowRequest& del = get<0>(*i);
        const void* user = get<1>(*i);
        dels[del.mTableName].push_back(user);
    }

    for(int32_t i = 0, sz = ores.GetTableItemSize(); i < sz; ++i) {
        const aliyun::openservices::ots::BatchWriteResponseTableItem& tbl =
            ores.GetTableItem(i);
        const string& tblName = tbl.GetTableName();
        if (puts.find(tblName) != puts.end()) {
            PutAggregator::const_iterator it = puts.find(tblName);
            const deque<const void*>& userData = it->second;
            OTS_ASSERT(mLogger, static_cast<int64_t>(userData.size()) == tbl.GetPutRowItemSize())
                (userData.size())
                (tbl.GetPutRowItemSize())
                (tblName);
            for(int32_t i = 0, sz = userData.size(); i < sz; ++i) {
                const aliyun::openservices::ots::BatchWriteResponseRowItem& oitem =
                    tbl.GetPutRowItem(i);
                const void* ud = userData[i];
                if (oitem.IsOK()) {
                    resp->mPutRows.push_back(make_tuple(Exceptional(), ud));
                } else {
                    const aliyun::openservices::ots::Error& err = oitem.GetError();
                    OTS_LOG_ERROR(mLogger)(err.GetCode())(err.GetMessage());
                    resp->mPutRows.push_back(
                        make_tuple(Exceptional(err), ud));
                }
            }
        }
        if (dels.find(tblName) != dels.end()) {
            DelAggregator::const_iterator it = dels.find(tblName);
            const deque<const void*>& userData = it->second;
            OTS_ASSERT(mLogger, static_cast<int64_t>(userData.size()) == tbl.GetDeleteRowItemSize())
                (userData.size())
                (tbl.GetDeleteRowItemSize())
                (tblName);
            for(int32_t i = 0, sz = userData.size(); i < sz; ++i) {
                const aliyun::openservices::ots::BatchWriteResponseRowItem& oitem =
                    tbl.GetDeleteRowItem(i);
                const void* ud = userData[i];
                if (oitem.IsOK()) {
                    resp->mDelRows.push_back(make_tuple(Exceptional(), ud));
                } else {
                    const aliyun::openservices::ots::Error& err = oitem.GetError();
                    OTS_LOG_ERROR(mLogger)(err.GetCode())(err.GetMessage());
                    resp->mDelRows.push_back(
                        make_tuple(Exceptional(err), ud));
                }
            }
        }
    }
}

void ClientDelegateImpl::Into(
    BatchGetRowResponse* resp,
    const aliyun::openservices::ots::BatchGetRowResponse& ores,
    const BatchGetRowRequest& req)
{
    typedef map<string, deque<tuple<GetRowRequest, const void*> > > GetAggregator;
    GetAggregator gets;
    FOREACH_ITER(i, req.mGetRows) {
        const GetRowRequest& g = get<0>(*i);
        gets[g.mTableName].push_back(*i);
    }

    for(int32_t i = 0, sz = ores.GetTableItemSize(); i < sz; ++i) {
        const aliyun::openservices::ots::BatchGetResponseTableItem& tbl =
            ores.GetTableItem(i);
        const string& tblName = tbl.GetTableName();
        GetAggregator::const_iterator it = gets.find(tblName);
        OTS_ASSERT(mLogger, it != gets.end())(tblName);
        const deque<tuple<GetRowRequest, const void*> >& userData = it->second;
        OTS_ASSERT(mLogger, static_cast<int64_t>(userData.size()) == tbl.GetRowSize())
            (userData.size())
            (tbl.GetRowSize())
            (tblName);
        for(int32_t i = 0, sz = userData.size(); i < sz; ++i) {
            const aliyun::openservices::ots::BatchGetResponseRowItem& oitem =
                tbl.GetRowItem(i);
            const GetRowRequest& req = get<0>(userData[i]);
            const void* ud = get<1>(userData[i]);
            if (oitem.IsOK()) {
                GetRowResponse res;
                res.mTracker = req.mTracker;
                res.mRow.reset(new Row());
                Into(res.mRow.get(), oitem.GetRow());
                resp->mGetRows.push_back(make_tuple(Exceptional(), res, ud));
            } else {
                const aliyun::openservices::ots::Error& err = oitem.GetError();
                OTS_LOG_ERROR(mLogger)(err.GetCode())(err.GetMessage());
                GetRowResponse res;
                res.mTracker = req.mTracker;
                res.mRow.reset(new Row());
                resp->mGetRows.push_back(
                    make_tuple(Exceptional(err), res, ud));
            }
        }
    }
}

void ClientDelegateImpl::Into(
    Row* out,
    const aliyun::openservices::ots::Row& in)
{
    out->mPrimaryKey.clear();
    for(int32_t i = 0, sz = in.GetPrimaryKeySize(); i < sz; ++i) {
        const aliyun::openservices::ots::Column& col = in.GetPrimaryKey(i);
        out->mPrimaryKey.push_back(Column(mLogger));
        Into(&(out->mPrimaryKey.back()), col);
    }
    out->mAttributes.clear();
    for(int32_t i = 0, sz = in.GetColumnSize(); i < sz; ++i) {
        const aliyun::openservices::ots::Column& col = in.GetColumn(i);
        out->mAttributes.push_back(Column(mLogger));
        Into(&(out->mAttributes.back()), col);
    }
}

void ClientDelegateImpl::Into(
    GetRangeResponse* resp,
    const aliyun::openservices::ots::GetRangeResponse& ores)
{
    resp->mNextPkey.clear();
    resp->mRows.clear();
    for(int32_t i = 0, sz = ores.GetNextStartPrimaryKeySize(); i < sz; ++i) {
        const aliyun::openservices::ots::Column& ocol =
            ores.GetNextStartPrimaryKey(i);
        Column col(mLogger);
        Into(&col, ocol);
        resp->mNextPkey.push_back(col);
    }
    for(int32_t i = 0, sz = ores.GetRowSize(); i < sz; ++i) {
        const aliyun::openservices::ots::Row& orow = ores.GetRow(i);
        resp->mRows.push_back(shared_ptr<Row>(new Row()));
        Into(resp->mRows.back().get(), orow);
    }
}

void ClientDelegateImpl::Into(
    Column* out,
    const aliyun::openservices::ots::Column& in)
{
    out->mName = in.GetColumnName();
    Into(&(out->mValue), in.GetColumnValue());
}

void ClientDelegateImpl::Into(
    Value* out,
    const aliyun::openservices::ots::OTSValue& in)
{
    switch(in.GetType()) {
    case aliyun::openservices::ots::INF_MIN: {
        *out = Value(mLogger, Value::InfMin());
        break;
    }
    case aliyun::openservices::ots::INF_MAX: {
        *out = Value(mLogger, Value::InfMax());
        break;
    }
    case aliyun::openservices::ots::INTEGER: {
        *out = Value(
            mLogger,
            dynamic_cast<const aliyun::openservices::ots::OTSIntValue&>(in).GetValue());
        break;
    }
    case aliyun::openservices::ots::STRING: {
        *out = Value(
            mLogger,
            dynamic_cast<const aliyun::openservices::ots::OTSStringValue&>(in).GetValue());
        break;
    }
    case aliyun::openservices::ots::BOOLEAN: {
        *out = Value(
            mLogger,
            dynamic_cast<const aliyun::openservices::ots::OTSBoolValue&>(in).GetValue());
        break;
    }
    case aliyun::openservices::ots::DOUBLE: {
        *out = Value(
            mLogger,
            dynamic_cast<const aliyun::openservices::ots::OTSDoubleValue&>(in).GetValue());
        break;
    }
    default: {
        OTS_ASSERT(mLogger, false)((int) in.GetType()).What("unsupported ots value type");
    }
    }
}

} // namespace

ClientDelegate* ClientDelegate::New(
    Logger* logger,
    const aliyun::openservices::ots::OTSConfig& cfg)
{
    return new ClientDelegateImpl(logger, cfg);
}

} // namespace static_index

