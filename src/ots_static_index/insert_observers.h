#ifndef OTS_STATIC_INDEX_INSERT_OBSERVERS_H
#define OTS_STATIC_INDEX_INSERT_OBSERVERS_H

#include "type_delegates.h"
#include "slice.h"
#include "observable.h"
#include "ots_static_index/exceptional.h"
#include <string>
#include <deque>
#include <stdint.h>

namespace Json {
class Value;
} // namespace Json

namespace static_index {
class Column;
class TableMeta;
class Logger;
namespace ast {
class Node;
} // namespace ast

typedef DefaultObserver<Column, Exceptional> ColumnObserver;
typedef DefaultObservable<Column, Exceptional> ColumnObservable;
class ColumnObservableObserver: public ColumnObserver, public ColumnObservable
{
public:
    explicit ColumnObservableObserver(Logger* logger)
      : ColumnObserver(logger)
    {}
};

class AttrValidator: public ColumnObservableObserver
{
    Logger* mLogger;
    const QloMap< ::std::string, Value::Type>& mExpects;

public:
    explicit AttrValidator(Logger* logger, const TableMeta& schema);
    ~AttrValidator() {}

private:
    void OnInnerNext(const Column& data);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
};

class AttrFilter: public ColumnObservableObserver
{
    Logger* mLogger;
    const QloMap< ::std::string, Value::Type>& mExpects;

public:
    explicit AttrFilter(Logger* logger, const TableMeta& schema);

private:
    void OnInnerNext(const Column& data);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
};

class Crc64IntTransformer: public ColumnObservableObserver
{
    Logger* mLogger;
    ::std::string mNewName;

public:
    explicit Crc64IntTransformer(Logger* logger, const ::std::string& newName);

private:
    void OnInnerNext(const Column& data);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
};

class Crc64StrTransformer: public ColumnObservableObserver
{
    Logger* mLogger;
    ::std::string mNewName;

public:
    explicit Crc64StrTransformer(Logger* logger, const ::std::string& newName);

private:
    void OnInnerNext(const Column& data);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
};

class HexTransformer: public ColumnObservableObserver
{
    Logger* mLogger;
    ::std::string mNewName;

public:
    explicit HexTransformer(Logger* logger, const ::std::string& newName);

private:
    void OnInnerNext(const Column& data);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
};

class ShiftToUint64Transformer: public ColumnObservableObserver
{
    Logger* mLogger;
    ::std::string mNewName;

public:
    explicit ShiftToUint64Transformer(Logger* logger, const ::std::string& newName);

private:
    void OnInnerNext(const Column& data);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
};

class ConcatTransformer: public ColumnObservableObserver
{
    Logger* mLogger;
    ::std::string mNewName;
    char mSep;
    QloMap< ::std::string, int64_t> mLocations;
    ::std::deque<Column> mReadyValues;
    int64_t mFilledCount;

public:
    explicit ConcatTransformer(Logger* logger, const ast::Node& root, char sep);

private:
    void OnInnerNext(const Column& data);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
    bool AllFilled() const;
};

class PickAttrTransformer: public ColumnObservableObserver
{
    Logger* mLogger;
    ::std::string mExpectName;

public:
    explicit PickAttrTransformer(Logger* logger, const ::std::string& name);

private:
    void OnInnerNext(const Column& data);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
};

class PutRowRequestFiller: public ColumnObserver
{
    Logger* mLogger;
    ::std::deque<PutRowRequest>* mOutputs;
    PutRowRequest mRequest;
    Exceptional mExcept;
    ::std::deque< ::std::string > mPkNames;
    QloMap<Slice, Column> mPks;
    bool mIsPrimary;

public:
    explicit PutRowRequestFiller(
        Logger* logger,
        ::std::deque<PutRowRequest>* outs,
        const TableMeta& schema,
        const ::std::string& track,
        bool isPrimary);
    virtual ~PutRowRequestFiller() {}

    const Exceptional& GetExcept()
    {
        return mExcept;
    }
    
private:
    void OnInnerNext(const Column& attr);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
    Slice FindMissingPkey() const;
};

class JsonValueToColumn: public ColumnObservable
{
    Logger* mLogger;

public:
    explicit JsonValueToColumn(Logger* logger);
    ~JsonValueToColumn() {}

    void Go(const ::std::string& tracker, const Json::Value& data);

private:
    void SetValue(Value*, const Json::Value&);
};

class DeleteRowRequestFiller: public ColumnObserver
{
    Logger* mLogger;
    ::std::deque<DeleteRowRequest>* mOutputs;
    DeleteRowRequest mRequest;
    Exceptional mExcept;
    ::std::deque< ::std::string > mPkNames;
    QloMap<Slice, Column> mPks;

public:
    explicit DeleteRowRequestFiller(
        Logger* logger,
        ::std::deque<DeleteRowRequest>* outs,
        const TableMeta& schema,
        const ::std::string& track);

    const Exceptional& GetExcept()
    {
        return mExcept;
    }
    
private:
    void OnInnerNext(const Column& attr);
    void OnInnerCompletion();
    void OnInnerError(const Exceptional& ex);
    Slice FindMissingPkey() const;
};

} // namespace static_index

#endif /* OTS_STATIC_INDEX_INSERT_OBSERVERS_H */
