#pragma once
#ifndef TABLESTORE_CORE_IMPL_API_TRAITS_HPP
#define TABLESTORE_CORE_IMPL_API_TRAITS_HPP
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
#include "tablestore/core/types.hpp"
#include <string>

namespace com {
namespace aliyun {
namespace tablestore {
namespace protocol {
class ListTableRequest;
class ListTableResponse;
class CreateTableRequest;
class CreateTableResponse;
class DeleteTableRequest;
class DeleteTableResponse;
class DescribeTableRequest;
class DescribeTableResponse;
class UpdateTableRequest;
class UpdateTableResponse;
class ComputeSplitPointsBySizeRequest;
class ComputeSplitPointsBySizeResponse;
class PutRowRequest;
class PutRowResponse;
class GetRowRequest;
class GetRowResponse;
class GetRangeRequest;
class GetRangeResponse;
class UpdateRowRequest;
class UpdateRowResponse;
class DeleteRowRequest;
class DeleteRowResponse;
class BatchGetRowRequest;
class BatchGetRowResponse;
class BatchWriteRowRequest;
class BatchWriteRowResponse;
} // namespace protocol
} // namespace tablestore
} // namespace aliyun
} // namespace com

namespace aliyun {
namespace tablestore {
namespace core {
namespace impl {

template<Action kAction>
struct ApiTraits {};

template<>
struct ApiTraits<kApi_ListTable>
{
    typedef aliyun::tablestore::core::ListTableRequest ApiRequest;
    typedef aliyun::tablestore::core::ListTableResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::ListTableRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::ListTableResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_CreateTable>
{
    typedef aliyun::tablestore::core::CreateTableRequest ApiRequest;
    typedef aliyun::tablestore::core::CreateTableResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::CreateTableRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::CreateTableResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_DeleteTable>
{
    typedef aliyun::tablestore::core::DeleteTableRequest ApiRequest;
    typedef aliyun::tablestore::core::DeleteTableResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::DeleteTableRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::DeleteTableResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_DescribeTable>
{
    typedef aliyun::tablestore::core::DescribeTableRequest ApiRequest;
    typedef aliyun::tablestore::core::DescribeTableResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::DescribeTableRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::DescribeTableResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_UpdateTable>
{
    typedef aliyun::tablestore::core::UpdateTableRequest ApiRequest;
    typedef aliyun::tablestore::core::UpdateTableResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::UpdateTableRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::UpdateTableResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_ComputeSplitsBySize>
{
    typedef aliyun::tablestore::core::ComputeSplitsBySizeRequest ApiRequest;
    typedef aliyun::tablestore::core::ComputeSplitsBySizeResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::ComputeSplitPointsBySizeRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::ComputeSplitPointsBySizeResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_PutRow>
{
    typedef aliyun::tablestore::core::PutRowRequest ApiRequest;
    typedef aliyun::tablestore::core::PutRowResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::PutRowRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::PutRowResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_GetRow>
{
    typedef aliyun::tablestore::core::GetRowRequest ApiRequest;
    typedef aliyun::tablestore::core::GetRowResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::GetRowRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::GetRowResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_GetRange>
{
    typedef aliyun::tablestore::core::GetRangeRequest ApiRequest;
    typedef aliyun::tablestore::core::GetRangeResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::GetRangeRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::GetRangeResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_UpdateRow>
{
    typedef aliyun::tablestore::core::UpdateRowRequest ApiRequest;
    typedef aliyun::tablestore::core::UpdateRowResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::UpdateRowRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::UpdateRowResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_DeleteRow>
{
    typedef aliyun::tablestore::core::DeleteRowRequest ApiRequest;
    typedef aliyun::tablestore::core::DeleteRowResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::DeleteRowRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::DeleteRowResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_BatchGetRow>
{
    typedef aliyun::tablestore::core::BatchGetRowRequest ApiRequest;
    typedef aliyun::tablestore::core::BatchGetRowResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::BatchGetRowRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::BatchGetRowResponse PbResponse;
    static const std::string kPath;
};

template<>
struct ApiTraits<kApi_BatchWriteRow>
{
    typedef aliyun::tablestore::core::BatchWriteRowRequest ApiRequest;
    typedef aliyun::tablestore::core::BatchWriteRowResponse ApiResponse;
    typedef com::aliyun::tablestore::protocol::BatchWriteRowRequest PbRequest;
    typedef com::aliyun::tablestore::protocol::BatchWriteRowResponse PbResponse;
    static const std::string kPath;
};

} // namespace impl
} // namespace core
} // namespace tablestore
} // namespace aliyun

#endif
