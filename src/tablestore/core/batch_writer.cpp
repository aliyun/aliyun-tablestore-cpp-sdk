#include "batch_writer.hpp"
#include "impl/async_batch_writer.hpp"
#include "tablestore/util/threading.hpp"
#include "tablestore/util/try.hpp"
#include <boost/ref.hpp>
#include <boost/noncopyable.hpp>
#include <tr1/functional>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {

BatchWriterConfig::BatchWriterConfig()
  : mMaxConcurrency(32),
    mMaxBatchSize(200),
    mRegularNap(util::Duration::fromMsec(10)),
    mMaxNap(util::Duration::fromSec(10)),
    mNapShrinkStep(util::Duration::fromMsec(157))
{
}

void BatchWriterConfig::prettyPrint(string& out) const
{
    pp::prettyPrint(out, "{\"MaxConcurrency\":");
    pp::prettyPrint(out, mMaxConcurrency);

    pp::prettyPrint(out, ",\"MaxBatchSize\":");
    pp::prettyPrint(out, mMaxBatchSize);

    pp::prettyPrint(out, ",\"RegularNap\":");
    pp::prettyPrint(out, mRegularNap);

    pp::prettyPrint(out, ",\"MaxNap\":");
    pp::prettyPrint(out, mMaxNap);

    pp::prettyPrint(out, ",\"NapShrinkStep\":");
    pp::prettyPrint(out, mNapShrinkStep);

    if (mActors.present()) {
        pp::prettyPrint(out, ",\"Actors\":");
        pp::prettyPrint(out, mActors->size());
    }

    out.push_back('}');
}

Optional<OTSError> BatchWriterConfig::validate() const
{
    if (mMaxConcurrency < 1) {
        OTSError e(OTSError::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = "Max concurrency must be positive.";
        return Optional<OTSError>(util::move(e));
    }
    if (mMaxBatchSize < 1) {
        OTSError e(OTSError::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = "Max batch size must be positive.";
        return Optional<OTSError>(util::move(e));
    }
    if (mRegularNap <= Duration::fromMsec(1)) {
        OTSError e(OTSError::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = "Regular nap must be greater than one msec.";
        return Optional<OTSError>(util::move(e));
    }
    if (mMaxNap < mRegularNap * 2) {
        OTSError e(OTSError::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = "Max nap must be longer than twice regular period.";
        return Optional<OTSError>(util::move(e));
    }
    if (mNapShrinkStep <= Duration::fromSec(0)) {
        OTSError e(OTSError::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = "Each step on shrinking nap must be positive.";
        return Optional<OTSError>(util::move(e));
    }
    if (mActors.present() && mActors->size() == 0) {
        OTSError e(OTSError::kPredefined_OTSParameterInvalid);
        e.mutableMessage() = "Number of invoking-callback threads must be positive.";
        return Optional<OTSError>(util::move(e));
    }
    return Optional<OTSError>();
}

namespace {
class SyncBatchWriterImpl : public SyncBatchWriter, private boost::noncopyable
{
public:
    explicit SyncBatchWriterImpl(AsyncBatchWriter* ac)
      : mAsyncWriter(ac)
    {}

    Optional<OTSError> putRow(PutRowResponse&, const PutRowRequest&);
    Optional<OTSError> updateRow(UpdateRowResponse&, const UpdateRowRequest&);
    Optional<OTSError> deleteRow(DeleteRowResponse&, const DeleteRowRequest&);

private:
    auto_ptr<AsyncBatchWriter> mAsyncWriter;
};

template<typename Request, typename Response>
void callback(
    Semaphore& sem,
    Optional<OTSError>& outErr,
    Response& outResp,
    Request& req,
    Optional<OTSError>& inErr,
    Response& inResp)
{
    moveAssign(outErr, util::move(inErr));
    moveAssign(outResp, util::move(inResp));
    sem.post();
}

Optional<OTSError> SyncBatchWriterImpl::putRow(
    PutRowResponse& resp,
    const PutRowRequest& reqIn)
{
    Optional<OTSError> err;
    PutRowRequest req = reqIn;
    Semaphore sem(0);
    mAsyncWriter->putRow(
        req,
        bind(callback<PutRowRequest, PutRowResponse>,
            boost::ref(sem),
            boost::ref(err),
            boost::ref(resp),
            _1, _2, _3));
    sem.wait();
    return err;
}

Optional<OTSError> SyncBatchWriterImpl::updateRow(
    UpdateRowResponse& resp,
    const UpdateRowRequest& reqIn)
{
    Optional<OTSError> err;
    UpdateRowRequest req = reqIn;
    Semaphore sem(0);
    mAsyncWriter->updateRow(
        req,
        bind(callback<UpdateRowRequest, UpdateRowResponse>,
            boost::ref(sem),
            boost::ref(err),
            boost::ref(resp),
            _1, _2, _3));
    sem.wait();
    return err;
}

Optional<OTSError> SyncBatchWriterImpl::deleteRow(
    DeleteRowResponse& resp,
    const DeleteRowRequest& reqIn)
{
    Optional<OTSError> err;
    DeleteRowRequest req = reqIn;
    Semaphore sem(0);
    mAsyncWriter->deleteRow(
        req,
        bind(callback<DeleteRowRequest, DeleteRowResponse>,
            boost::ref(sem),
            boost::ref(err),
            boost::ref(resp),
            _1, _2, _3));
    sem.wait();
    return err;
}

} // namespace

Optional<OTSError> SyncBatchWriter::create(
    SyncBatchWriter*& writer,
    AsyncClient& client,
    const BatchWriterConfig& cfg)
{
    AsyncBatchWriter* ac = NULL;
    TRY(AsyncBatchWriter::create(ac, client, cfg));
    writer = new SyncBatchWriterImpl(ac);
    return Optional<OTSError>();
}

Optional<OTSError> AsyncBatchWriter::create(
    AsyncBatchWriter*& writer,
    AsyncClient& client,
    const BatchWriterConfig& cfg)
{
    TRY(cfg.validate());
    writer = new impl::AsyncBatchWriter(client, cfg);
    return Optional<OTSError>();
}

} // namespace core
} // namespace tablestore
} // namespace aliyun
