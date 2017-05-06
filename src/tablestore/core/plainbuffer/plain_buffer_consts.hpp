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
#pragma once

#include <stdint.h>

namespace aliyun {
namespace tablestore {
namespace core {

const int32_t HEADER = 0x75;

// tag type
const int8_t TAG_ROW_PK = 0x1;
const int8_t TAG_ROW_DATA = 0x2;
const int8_t TAG_CELL = 0x3;
const int8_t TAG_CELL_NAME = 0x4;
const int8_t TAG_CELL_VALUE = 0x5;
const int8_t TAG_CELL_TYPE = 0x6;
const int8_t TAG_CELL_TIMESTAMP = 0x7;
const int8_t TAG_DELETE_ROW_MARKER = 0x8;
const int8_t TAG_ROW_CHECKSUM = 0x9;
const int8_t TAG_CELL_CHECKSUM = 0x0A;

// cell op type
const int8_t DELETE_ALL_VERSION = 0x1;
const int8_t DELETE_ONE_VERSION = 0x3;

// variant type
const int8_t VT_INTEGER = 0x0;
const int8_t VT_DOUBLE = 0x1;
const int8_t VT_BOOLEAN = 0x2;
const int8_t VT_STRING = 0x3;
const int8_t VT_NULL = 0x6;
const int8_t VT_BLOB = 0x7;
const int8_t VT_INF_MIN = 0x9;
const int8_t VT_INF_MAX = 0xa;
const int8_t VT_AUTO_INCREMENT = 0xb;

} // namespace core
} // namespace tablestore
} // namespace aliyun
