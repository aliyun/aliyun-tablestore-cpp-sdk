#include "insert_observers.h"
#include "def_ast.h"
#include "security.h"
#include "arithmetic.h"
#include "jsoncpp/json/writer.h"

using namespace ::std;
using namespace ::std::tr1;

#define OBS_TRY(err) \
    if (err.GetCode() != ::static_index::Exceptional::OTS_OK) { \
        NotifyError(err);                                       \
        return;                                                 \
    }

namespace static_index {

AttrValidator::AttrValidator(Logger* logger, const TableMeta& schema)
  : ColumnObservableObserver(logger),
    mLogger(logger),
    mExpects(schema.mAttributeTypes)
{
}

void AttrValidator::OnInnerNext(const Column& data)
{
    QloMap<string, Value::Type>::const_iterator it = mExpects.find(data.mName);
    if (it != mExpects.end()) {
        if (data.mValue.GetType() != it->second) {
            OTS_LOG_ERROR(mLogger)
                (data.mName)
                ((int) data.mValue.GetType())
                ((int) it->second)
                .What("mismatch type of attribute");
            OBS_TRY(Exceptional("mismatch type of attribute " + data.mName));
        }
    }
    NotifyNext(data);
}
void AttrValidator::OnInnerCompletion()
{
    NotifyCompletion();
}
void AttrValidator::OnInnerError(const Exceptional& ex)
{
    NotifyError(ex);
}


AttrFilter::AttrFilter(
    Logger* logger,
    const TableMeta& schema)
  : ColumnObservableObserver(logger),
    mLogger(logger),
    mExpects(schema.mAttributeTypes)
{
}

void AttrFilter::OnInnerNext(const Column& data)
{
    if (mExpects.find(data.mName) != mExpects.end()) {
        NotifyNext(data);
    }
}
void AttrFilter::OnInnerCompletion()
{
    NotifyCompletion();
}
void AttrFilter::OnInnerError(const Exceptional& ex)
{
    NotifyError(ex);
}


Crc64IntTransformer::Crc64IntTransformer(
    Logger* logger,
    const string& newName)
  : ColumnObservableObserver(logger),
    mLogger(logger),
    mNewName(newName)
{}

void Crc64IntTransformer::OnInnerNext(const Column& data)
{
    uint64_t x = data.mValue.Int();
    uint64_t y = Crc64Int(x);
    Column newCol(mLogger);
    newCol.mName = mNewName;
    newCol.mValue.SetInt(y);
    NotifyNext(newCol);
}
void Crc64IntTransformer::OnInnerCompletion()
{
    NotifyCompletion();
}
void Crc64IntTransformer::OnInnerError(const Exceptional& ex)
{
    NotifyError(ex);
}

Crc64StrTransformer::Crc64StrTransformer(
    Logger* logger,
    const string& newName)
  : ColumnObservableObserver(logger),
    mLogger(logger),
    mNewName(newName)
{}

void Crc64StrTransformer::OnInnerNext(const Column& data)
{
    string x = data.mValue.Str();
    uint64_t y = Crc64Str(ToSlice(x));
    Column newCol(mLogger);
    newCol.mName = mNewName;
    newCol.mValue.SetInt(y);
    NotifyNext(newCol);
}
void Crc64StrTransformer::OnInnerCompletion()
{
    NotifyCompletion();
}
void Crc64StrTransformer::OnInnerError(const Exceptional& ex)
{
    NotifyError(ex);
}


HexTransformer::HexTransformer(
    Logger* logger,
    const string& newName)
  : ColumnObservableObserver(logger),
    mLogger(logger),
    mNewName(newName)
{}
void HexTransformer::OnInnerNext(const Column& data)
{
    uint64_t x = data.mValue.Int();
    string y = Hex(x);
    Column newCol(mLogger);
    newCol.mName = mNewName;
    newCol.mValue.SetStr(y);
    NotifyNext(newCol);
}
void HexTransformer::OnInnerCompletion()
{
    NotifyCompletion();
}
void HexTransformer::OnInnerError(const Exceptional& ex)
{
    NotifyError(ex);
}

ShiftToUint64Transformer::ShiftToUint64Transformer(
    Logger* logger,
    const string& newName)
  : ColumnObservableObserver(logger),
    mLogger(logger),
    mNewName(newName)
{}
void ShiftToUint64Transformer::OnInnerNext(const Column& data)
{
    int64_t x = data.mValue.Int();
    uint64_t y = ShiftToUint64(x);
    Column newCol(mLogger);
    newCol.mName = mNewName;
    newCol.mValue.SetInt(y);
    NotifyNext(newCol);
}
void ShiftToUint64Transformer::OnInnerCompletion()
{
    NotifyCompletion();
}
void ShiftToUint64Transformer::OnInnerError(const Exceptional& ex)
{
    NotifyError(ex);
}


ConcatTransformer::ConcatTransformer(
    Logger* logger,
    const ast::Node& root,
    char sep)
  : ColumnObservableObserver(logger),
    mLogger(logger),
    mNewName(root.mName),
    mSep(sep),
    mFilledCount(0)
{
    int64_t loc = 0;
    FOREACH_ITER(i, root.mChildren) {
        const shared_ptr<ast::Node>& n = *i;
        mReadyValues.push_back(Column(mLogger));
        mLocations[n->mName] = loc;
        ++loc;
    }
}
void ConcatTransformer::OnInnerNext(const Column& data)
{
    OTS_LOG_DEBUG(mLogger)(data.mName).What("Incoming column");
    QloMap<string, int64_t>::const_iterator it = mLocations.find(data.mName);
    OTS_ASSERT(mLogger, it != mLocations.end())(data.mName)(mNewName);
    Column& value = mReadyValues.at(it->second);
    if (value.mValue.GetType() != Value::INVALID) {
        // already filled, just ignore
        return;
    }

    string x = data.mValue.Str();
    Column newCol(mLogger);
    newCol.mValue.SetStr(x);
    value = newCol;
    ++mFilledCount;

    if (AllFilled()) {
        string res;
        FOREACH_ITER(i, mReadyValues) {
            if (i != mReadyValues.begin()) {
                res.push_back(mSep);
            }
            const Column& c = *i;
            res.append(c.mValue.Str());
        }
        Column newCol(mLogger);
        newCol.mName = mNewName;
        newCol.mValue.SetStr(res);
        NotifyNext(newCol);
    }
}
void ConcatTransformer::OnInnerCompletion()
{
    if (AllFilled()) {
        OTS_LOG_DEBUG(mLogger)(mNewName).What("CONCAT completion");
        NotifyCompletion();
    } else {
        OTS_LOG_INFO(mLogger)
            (mNewName)
            (mReadyValues.size())
            (mFilledCount)
            .What("CONCAT uncompleted");
    }
}
void ConcatTransformer::OnInnerError(const Exceptional& ex)
{
    NotifyError(ex);
}
bool ConcatTransformer::AllFilled() const
{
    return mFilledCount == static_cast<int64_t>(mReadyValues.size());
}


PickAttrTransformer::PickAttrTransformer(Logger* logger, const string& name)
  : ColumnObservableObserver(logger),
    mLogger(logger),
    mExpectName(name)
{}
void PickAttrTransformer::OnInnerNext(const Column& data)
{
    if (ToSlice(data.mName) == ToSlice(mExpectName)) {
        NotifyNext(data);
    }
}
void PickAttrTransformer::OnInnerCompletion()
{
    NotifyCompletion();
}
void PickAttrTransformer::OnInnerError(const Exceptional& ex)
{
    NotifyError(ex);
}


PutRowRequestFiller::PutRowRequestFiller(
    Logger* logger,
    deque<PutRowRequest>* outs,
    const TableMeta& schema,
    const string& track,
    bool isPrimary)
  : ColumnObserver(logger),
    mLogger(logger),
    mOutputs(outs),
    mIsPrimary(isPrimary)
{
    mRequest.mTableName = schema.mTableName;
    mRequest.mCondition.mRowExist = aliyun::openservices::ots::IGNORE;

    FOREACH_ITER(i, schema.mPrimaryKey) {
        mPkNames.push_back((*i)->mName);
    }

    FOREACH_ITER(i, mPkNames) {
        const string& pkName = *i;
        Column c(mLogger);
        c.mName = *i;
        bool ret = mPks.insert(make_pair(ToSlice(pkName), c)).second;
        OTS_ASSERT(mLogger, ret)(pkName);
    }

    Column attrTrack(mLogger);
    attrTrack.mName = "_track";
    attrTrack.mValue = Value(mLogger, track);
    mRequest.mAttributes.push_back(attrTrack);
}
void PutRowRequestFiller::OnInnerNext(const Column& attr)
{
    const string& name = attr.mName;
    QloMap<Slice, Column>::iterator it = mPks.find(ToSlice(name));
    if (it == mPks.end()) {
        OTS_LOG_DEBUG(mLogger)
            (ToString(Jsonize(mLogger, attr)))
            .What("new attr");
        mRequest.mAttributes.push_back(attr);
    } else {
        OTS_LOG_DEBUG(mLogger)
            (ToString(Jsonize(mLogger, attr)))
            .What("new pkey");
        it->second = attr;
    }
}
void PutRowRequestFiller::OnInnerCompletion()
{
    const Slice& name = FindMissingPkey();
    if (name.Size() == 0) {
        // all pkey columns are filled
        FOREACH_ITER(i, mPkNames) {
            const string& pkName = *i;
            QloMap<Slice, Column>::const_iterator ci =
                mPks.find(ToSlice(pkName));
            OTS_ASSERT(mLogger, ci != mPks.end())(pkName);

            mRequest.mPrimaryKey.push_back(ci->second);
        }
        OTS_LOG_DEBUG(mLogger)
            (ToString(Jsonize(mLogger, mRequest)))
            .What("new PutRowRequest");
        mOutputs->push_back(mRequest);
    } else {
        if (mIsPrimary) {
            OTS_LOG_ERROR(mLogger)
                (ToString(name))
                .What("Missing required attribute");
            mExcept = Exceptional("Missing required attribute: " + ToString(name));
        } else {
            // for indexes, just ignore
            OTS_LOG_INFO(mLogger)
                (ToString(name))
                .What("Missing required attribute");
        }
    }
}
void PutRowRequestFiller::OnInnerError(const Exceptional& ex)
{
    mExcept = ex;
}

Slice PutRowRequestFiller::FindMissingPkey() const
{
    FOREACH_ITER(i, mPks) {
        const Slice name = i->first;
        const Column& col = i->second;
        if (col.mValue.GetType() == Value::INVALID) {
            return name;
        }
    }
    return Slice();
}

JsonValueToColumn::JsonValueToColumn(Logger* logger)
  : mLogger(logger)
{}
void JsonValueToColumn::Go(const string& track, const Json::Value& data)
{
    const vector<string>& members = data.getMemberNames();
    FOREACH_ITER(i, members) {
        const string& key = *i;
        const Json::Value& val = data[key];
        OTS_LOG_DEBUG(mLogger)
            (track)
            (key)
            (ToString(val))
            .What("Issue a new column");

        Column col(mLogger);
        col.mName = key;
        SetValue(&(col.mValue), val);

        NotifyNext(col);
    }
    NotifyCompletion();
}
void JsonValueToColumn::SetValue(Value* out, const Json::Value& val)
{
    if (val.isArray() || val.isObject()) {
        Json::FastWriter writer;
        out->SetStr(writer.write(val));
    } else {
        Unjsonize(mLogger, out, val);
    }
}


DeleteRowRequestFiller::DeleteRowRequestFiller(
    Logger* logger,
    deque<DeleteRowRequest>* outs,
    const TableMeta& schema,
    const string& track)
  : ColumnObserver(logger),
    mLogger(logger),
    mOutputs(outs)
{
    mRequest.mTracker = track;
    mRequest.mTableName = schema.mTableName;
    mRequest.mCondition.mRowExist = aliyun::openservices::ots::IGNORE;

    FOREACH_ITER(i, schema.mPrimaryKey) {
        mPkNames.push_back((*i)->mName);
    }

    FOREACH_ITER(i, mPkNames) {
        const string& pkName = *i;
        Column c(mLogger);
        c.mName = *i;
        bool ret = mPks.insert(make_pair(ToSlice(pkName), c)).second;
        OTS_ASSERT(mLogger, ret)(pkName);
    }
}
void DeleteRowRequestFiller::OnInnerNext(const Column& attr)
{
    const string& name = attr.mName;
    QloMap<Slice, Column>::iterator it = mPks.find(ToSlice(name));
    if (it == mPks.end()) {
        OTS_LOG_DEBUG(mLogger)
            (ToString(Jsonize(mLogger, attr)))
            .What("ignore attr");
    } else {
        OTS_LOG_DEBUG(mLogger)
            (ToString(Jsonize(mLogger, attr)))
            .What("new pkey");
        it->second = attr;
    }
}
void DeleteRowRequestFiller::OnInnerCompletion()
{
    const Slice& name = FindMissingPkey();
    if (name.Size() == 0) {
        // all pkey columns are filled
        FOREACH_ITER(i, mPkNames) {
            const string& pkName = *i;
            QloMap<Slice, Column>::const_iterator ci =
                mPks.find(ToSlice(pkName));
            OTS_ASSERT(mLogger, ci != mPks.end())(pkName);

            mRequest.mPrimaryKey.push_back(ci->second);
        }
        OTS_LOG_DEBUG(mLogger)
            (ToString(Jsonize(mLogger, mRequest)))
            .What("new PutRowRequest");
        mOutputs->push_back(mRequest);
    } else {
        // just ignore
        OTS_LOG_INFO(mLogger)
            (ToString(name))
            .What("Missing required attribute");
    }
}
void DeleteRowRequestFiller::OnInnerError(const Exceptional& ex)
{
    mExcept = ex;
}

Slice DeleteRowRequestFiller::FindMissingPkey() const
{
    FOREACH_ITER(i, mPks) {
        const Slice name = i->first;
        const Column& col = i->second;
        if (col.mValue.GetType() == Value::INVALID) {
            return name;
        }
    }
    return Slice();
}

} // namespace static_index

#undef OBS_TRY

