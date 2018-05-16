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
#ifndef TABLESTORE_CORE_PLAINBUFFER_PLAIN_BUFFER_CONSTS_HPP
#define TABLESTORE_CORE_PLAINBUFFER_PLAIN_BUFFER_CONSTS_HPP

#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {
namespace plainbuffer {

const uint32_t kHeader = 0x75;

enum Tag {
    kTag_None = 0,
    kTag_RowKey = 0x1,
    kTag_RowData = 0x2,
    kTag_Cell = 0x3,
    kTag_CellName = 0x4,
    kTag_CellValue = 0x5,
    kTag_CellType = 0x6,
    kTag_CellTimestamp = 0x7,
    kTag_RowDeleteMarker = 0x8,
    kTag_RowChecksum = 0x9,
    kTag_CellChecksum = 0x0A,
};

enum CellDeleteMarker {
    kCDM_DeleteAllVersions = 0x1,
    kCDM_DeleteOneVersion = 0x3,
};

enum VariantType {
    kVT_Integer = 0x0,
    kVT_Double = 0x1,
    kVT_Boolean = 0x2,
    kVT_String = 0x3,
    kVT_Null = 0x6,
    kVT_Blob = 0x7,
    kVT_InfMin = 0x9,
    kVT_InfMax = 0xa,
    kVT_AutoIncrement = 0xb,
};

} // namespace plainbuffer
} // namespace core
} // namespace tablestore
} // namespace aliyun
#endif
