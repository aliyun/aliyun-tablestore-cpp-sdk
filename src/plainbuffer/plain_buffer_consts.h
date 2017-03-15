/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

#ifndef PLAIN_BUFFER_CONSTS_H
#define PLAIN_BUFFER_CONSTS_H

#include "../coreimpl/ots_helper.h"
#include <string>

namespace aliyun {
namespace tablestore {

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

} // end of tablestore
} // end of aliyun

#endif
