#pragma once
/* 
BSD 3-Clause License

Copyright (c) 2017, Alibaba Cloud
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "api_traits.hpp"
#include "../protocol/zero_copy_stream.hpp"
#include "tablestore/core/error.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/optional.hpp"
#include "tablestore/util/mempiece.hpp"
#include <memory>
#include <string>
#include <map>

namespace com {
namespace aliyun {
namespace tablestore {
namespace protocol {
class Error;
} // namespace protocol
} // namespace tablestore
} // namespace aliyun
} // namespace com

namespace aliyun {
namespace tablestore {

namespace util {
class MemPiece;
class MemPool;
} // namespace util

namespace core {

class ListTableRequest;
class ListTableResponse;

namespace impl {

util::Optional<Error> deserialize(Error&, std::deque<util::MemPiece>&);

template<Action kAction>
class Serde {};

template<>
class Serde<kApi_ListTable>
{
public:
    typedef typename ApiTraits<kApi_ListTable>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_ListTable>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_ListTable>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_ListTable>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_CreateTable>
{
public:
    typedef typename ApiTraits<kApi_CreateTable>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_CreateTable>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_CreateTable>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_CreateTable>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_DeleteTable>
{
public:
    typedef typename ApiTraits<kApi_DeleteTable>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_DeleteTable>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_DeleteTable>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_DeleteTable>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_DescribeTable>
{
public:
    typedef typename ApiTraits<kApi_DescribeTable>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_DescribeTable>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_DescribeTable>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_DescribeTable>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_UpdateTable>
{
public:
    typedef typename ApiTraits<kApi_UpdateTable>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_UpdateTable>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_UpdateTable>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_UpdateTable>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_ComputeSplitsBySize>
{
public:
    typedef typename ApiTraits<kApi_ComputeSplitsBySize>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_ComputeSplitsBySize>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_ComputeSplitsBySize>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_ComputeSplitsBySize>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_PutRow>
{
public:
    typedef typename ApiTraits<kApi_PutRow>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_PutRow>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_PutRow>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_PutRow>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_GetRow>
{
public:
    typedef typename ApiTraits<kApi_GetRow>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_GetRow>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_GetRow>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_GetRow>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_GetRange>
{
public:
    typedef typename ApiTraits<kApi_GetRange>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_GetRange>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_GetRange>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_GetRange>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_UpdateRow>
{
public:
    typedef typename ApiTraits<kApi_UpdateRow>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_UpdateRow>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_UpdateRow>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_UpdateRow>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_DeleteRow>
{
public:
    typedef typename ApiTraits<kApi_DeleteRow>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_DeleteRow>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_DeleteRow>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_DeleteRow>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
};

template<>
class Serde<kApi_BatchGetRow>
{
public:
    typedef typename ApiTraits<kApi_BatchGetRow>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_BatchGetRow>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_BatchGetRow>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_BatchGetRow>::PbResponse PbResponse;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
    const ApiRequest* mRequest; // not own
};

template<>
class Serde<kApi_BatchWriteRow>
{
public:
    typedef typename ApiTraits<kApi_BatchWriteRow>::ApiRequest ApiRequest;
    typedef typename ApiTraits<kApi_BatchWriteRow>::ApiResponse ApiResponse;
    typedef typename ApiTraits<kApi_BatchWriteRow>::PbRequest PbRequest;
    typedef typename ApiTraits<kApi_BatchWriteRow>::PbResponse PbResponse;

    struct Index
    {
        std::deque<int64_t> mPuts;
        std::deque<int64_t> mUpdates;
        std::deque<int64_t> mDeletes;
    };

    typedef std::map<std::string, Index, util::QuasiLexicographicLess<std::string> > Indices;

    explicit Serde(util::MemPool&);
    util::Optional<Error> serialize(std::deque<util::MemPiece>&, const ApiRequest&);
    util::Optional<Error> deserialize(ApiResponse&, std::deque<util::MemPiece>&);

private:
    MemPoolZeroCopyOutputStream mOStream;
    const ApiRequest* mRequest; // not own
    Indices mIndices;
};

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun

