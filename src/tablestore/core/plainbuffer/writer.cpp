#include "writer.hpp"
#include "consts.hpp"
#include "tablestore/core/types.hpp"
#include "tablestore/util/security.hpp"
#include "tablestore/util/try.hpp"
#include "tablestore/util/foreach.hpp"
#include <boost/static_assert.hpp>
#include <tr1/type_traits>
#include <cstring>

using namespace std;
using namespace aliyun::tablestore::util;

namespace aliyun {
namespace tablestore {
namespace core {
namespace plainbuffer {

namespace {
void writeUint8(string& out, uint8_t val)
{
    out.push_back(static_cast<char>(val));
}

template<class T>
void writeUint(string& out, T val)
{
    BOOST_STATIC_ASSERT(std::tr1::is_unsigned<T>::value);

    for(size_t i = 0; i < sizeof(T); ++i) {
        uint8_t x = val & 0xff;
        writeUint8(out, x);
        val >>= 8;
    }
}

void writeUint32(string& out, uint32_t val)
{
    writeUint<uint32_t>(out, val);
}

void writeUint64(string& out, uint64_t val)
{
    writeUint<uint64_t>(out, val);
}

void write(string& out, VariantType vt)
{
    writeUint8(out, static_cast<uint8_t>(vt));
}

void write(string& out, Tag tag)
{
    writeUint8(out, static_cast<uint8_t>(tag));
}

void write(string& out, const MemPiece& p)
{
    writeUint32(out, p.length());
    out.append(reinterpret_cast<const char*>(p.data()), p.length());
}

void writeFpValue(string& out, double val)
{
    uint64_t x = 0;
    OTS_ASSERT(sizeof(x) == sizeof(val))
        (sizeof(x))
        (sizeof(val));
    memcpy(&x, &val, sizeof(x));
    writeUint64(out, x);
}

void writeFpValue(string& out, uint8_t& checksum, double val)
{
    uint64_t x = 0;
    OTS_ASSERT(sizeof(x) == sizeof(val))
        (sizeof(x))
        (sizeof(val));
    memcpy(&x, &val, sizeof(x));

    writeUint32(out, 1 + sizeof(uint64_t));
    write(out, kVT_Double) ;
    writeUint64(out, x);
    crc8(checksum, static_cast<uint8_t>(kVT_Double));
    crc8U64(checksum, x);
}

void writeHeader(string& out)
{
    writeUint32(out, kHeader);
}

void writeCellName(string& out, uint8_t& checksum, const string& in)
{
    write(out, kTag_CellName);
    write(out, MemPiece::from(in));
    crc8MemPiece(checksum, MemPiece::from(in));
}

void writeStrBlobValue(
    string& out,
    uint8_t& checksum,
    const MemPiece& p,
    VariantType vt)
{
    writeUint32(out, 1 + sizeof(uint32_t) + p.length());
    write(out, vt) ;
    write(out, p);
    crc8(checksum, static_cast<uint8_t>(vt));
    crc8U32(checksum, static_cast<uint32_t>(p.length()));
    crc8MemPiece(checksum, p);
}

void writeIntValue(string& out, uint8_t& checksum, uint64_t in)
{
    writeUint32(out, 1 + sizeof(uint64_t));
    write(out, kVT_Integer) ;
    writeUint64(out, in);
    crc8(checksum, static_cast<uint8_t>(kVT_Integer));
    crc8U64(checksum, in);
}

void writeSpecialPrimaryKeyValue(string& out, uint8_t& checksum, VariantType vt)
{
    writeUint32(out, 1);
    write(out, vt);
    crc8(checksum, static_cast<uint8_t>(vt));
}

void write(string& out, uint8_t& checksum, const PrimaryKeyValue& in)
{
    write(out, kTag_CellValue);

    switch (in.category()) {
    case PrimaryKeyValue::kInteger: {
        writeIntValue(out, checksum, in.integer());
        break;
    }
    case PrimaryKeyValue::kString: {
        const MemPiece& p = MemPiece::from(in.str());
        writeStrBlobValue(out, checksum, p, kVT_String);
        break;
    }
    case PrimaryKeyValue::kBinary: {
        const MemPiece& p = MemPiece::from(in.blob());
        writeStrBlobValue(out, checksum, p, kVT_Blob);
        break;
    }
    case PrimaryKeyValue::kInfMin:
        writeSpecialPrimaryKeyValue(out, checksum, kVT_InfMin);
        break;
    case PrimaryKeyValue::kInfMax:
        writeSpecialPrimaryKeyValue(out, checksum, kVT_InfMax);
        break;
    case PrimaryKeyValue::kAutoIncr:
        writeSpecialPrimaryKeyValue(out, checksum, kVT_AutoIncrement);
        break;

    case PrimaryKeyValue::kNone:
        OTS_ASSERT(false)(in);
    }
}

void writeCellChecksum(string& out, uint8_t checksum)
{
    write(out, kTag_CellChecksum);
    writeUint8(out, checksum);
}

void write(string& out, uint8_t& checksum, const PrimaryKeyColumn& in)
{
    uint8_t cellChecksum = 0;
    write(out, kTag_Cell);
    writeCellName(out, cellChecksum, in.name());
    write(out, cellChecksum, in.value());
    writeCellChecksum(out, cellChecksum);
    crc8(checksum, cellChecksum);
}

void write(string& out, uint8_t& checksum, const PrimaryKey& in)
{
    write(out, kTag_RowKey);
    for(int64_t i = 0, sz = in.size(); i < sz; ++i) {
        write(out, checksum, in[i]);
    }
}

void write(string& out, uint8_t& checksum, const AttributeValue& in)
{
    write(out, kTag_CellValue);

    switch (in.category()) {
    case AttributeValue::kInteger: {
        writeIntValue(out, checksum, in.integer());
        break;
    }
    case AttributeValue::kString: {
        const MemPiece& p = MemPiece::from(in.str());
        writeStrBlobValue(out, checksum, p, kVT_String);
        break;
    }
    case AttributeValue::kBinary: {
        const MemPiece& p = MemPiece::from(in.blob());
        writeStrBlobValue(out, checksum, p, kVT_Blob);
        break;
    }
    case AttributeValue::kFloatPoint: {
        writeFpValue(out, checksum, in.floatPoint());
        break;
    }
    case AttributeValue::kBoolean: {
        uint8_t x = in.boolean() ? 1 : 0;
        writeUint32(out, 2);
        write(out, kVT_Boolean) ;
        writeUint8(out, x);
        crc8(checksum, static_cast<uint8_t>(kVT_Boolean));
        crc8(checksum, x);
        break;
    }
    case AttributeValue::kNone:
        OTS_ASSERT(false)(in);
        break;
    }
}

void write(string& out, uint8_t& checksum, UtcTime in)
{
    write(out, kTag_CellTimestamp);
    uint64_t tm = in.toMsec();
    writeUint64(out, tm);
    crc8U64(checksum, tm);
}

void write(string& out, uint8_t& checksum, const Attribute& in)
{
    uint8_t cellChecksum = 0;
    write(out, kTag_Cell);
    writeCellName(out, cellChecksum, in.name());
    write(out, cellChecksum, in.value());
    if (in.timestamp().present()) {
        write(out, cellChecksum, *in.timestamp());
    }
    writeCellChecksum(out, cellChecksum);
    crc8(checksum, cellChecksum);
}

void write(string& out, uint8_t& checksum, const IVector<Attribute>& in)
{
    if (in.size() > 0) {
        write(out, kTag_RowData);
        for(int64_t i = 0, sz = in.size(); i < sz; ++i) {
            write(out, checksum, in[i]);
        }
    }
}

void writeDeleteMarker(string& out, uint8_t& checksum)
{
    write(out, kTag_RowDeleteMarker);
    crc8(checksum, 1);
}

void writeRowChecksum(string& out, uint8_t checksum)
{
    write(out, kTag_RowChecksum);
    writeUint8(out, checksum);
}

void write(string& out, CellDeleteMarker cdm)
{
    write(out, kTag_CellType);
    writeUint8(out, static_cast<uint8_t>(cdm));
}

void write(string& out, uint8_t& checksum, const RowUpdateChange::Update& in)
{
    uint8_t cellChecksum = 0;
    write(out, kTag_Cell);
    writeCellName(out, cellChecksum, in.attrName());
    if (in.attrValue().present()) {
        write(out, cellChecksum, *in.attrValue());
    }

    switch(in.type()) {
    case RowUpdateChange::Update::kPut:
        break;
    case RowUpdateChange::Update::kDelete:
        write(out, kCDM_DeleteOneVersion);
        break;
    case RowUpdateChange::Update::kDeleteAll:
        write(out, kCDM_DeleteAllVersions);
        break;
    }

    if (in.timestamp().present()) {
        write(out, kTag_CellTimestamp);
        uint64_t tm = in.timestamp()->toMsec();
        writeUint64(out, tm);
    }

    // the order of fields is different with the order of crc
    if (in.timestamp().present()) {
        uint64_t tm = in.timestamp()->toMsec();
        crc8U64(cellChecksum, tm);
    }

    switch(in.type()) {
    case RowUpdateChange::Update::kPut:
        break;
    case RowUpdateChange::Update::kDelete:
        crc8(cellChecksum, static_cast<uint8_t>(kCDM_DeleteOneVersion));
        break;
    case RowUpdateChange::Update::kDeleteAll:
        crc8(cellChecksum, static_cast<uint8_t>(kCDM_DeleteAllVersions));
        break;
    }

    writeCellChecksum(out, cellChecksum);
    crc8(checksum, cellChecksum);
}

void write(
    string& out,
    uint8_t& checksum,
    const IVector<RowUpdateChange::Update>& in)
{
    if (in.size() > 0) {
        write(out, kTag_RowData);
        for(int64_t i = 0, sz = in.size(); i < sz; ++i) {
            write(out, checksum, in[i]);
        }
    }
}

} // namespace

std::string& write(string& out, const AttributeValue& value)
{
    switch (value.category()) {
    case AttributeValue::kInteger:
        write(out, kVT_Integer) ;
        writeUint64(out, value.integer());
        break;
    case AttributeValue::kString: {
        const MemPiece& val = MemPiece::from(value.str());
        write(out, kVT_String) ;
        write(out, val);
        break;
    }
    case AttributeValue::kBinary: {
        const MemPiece& val = MemPiece::from(value.blob());
        write(out, kVT_Blob) ;
        write(out, val);
        break;
    }
    case AttributeValue::kBoolean: {
        write(out, kVT_Boolean) ;
        writeUint8(out, value.boolean() ? 1 : 0);
        break;
    }
    case AttributeValue::kFloatPoint: {
        write(out, kVT_Double);
        writeFpValue(out, value.floatPoint());
        break;
    }

    case AttributeValue::kNone:
        OTS_ASSERT(false)(value);
        break;
    }

    return out;
}

std::string& write(string& out, const PrimaryKeyValue& value)
{
    switch (value.category()) {
    case PrimaryKeyValue::kInfMin:
        write(out, kVT_InfMin) ;
        break;
    case PrimaryKeyValue::kInfMax:
        write(out, kVT_InfMax) ;
        break;
    case PrimaryKeyValue::kAutoIncr:
        write(out, kVT_AutoIncrement) ;
        break;
    case PrimaryKeyValue::kInteger: {
        write(out, kVT_Integer) ;
        writeUint64(out, value.integer());
        break;
    }
    case PrimaryKeyValue::kString: {
        const MemPiece& val = MemPiece::from(value.str());
        write(out, kVT_String);
        write(out, val);
        break;
    }
    case PrimaryKeyValue::kBinary: {
        const MemPiece& val = MemPiece::from(value.blob());
        write(out, kVT_Blob);
        write(out, val);
        break;
    }

    case PrimaryKeyValue::kNone:
        OTS_ASSERT(false)(value);
    }

    return out;
}

std::string& write(string& out, const PrimaryKey& in)
{
    uint8_t checksum = 0;
    writeHeader(out);
    write(out, checksum, in);
    crc8(checksum, 0); // placeholder for delete marker
    writeRowChecksum(out, checksum);
    return out;
}

std::string& write(string& out, const RowPutChange& in)
{
    uint8_t checksum = 0;
    writeHeader(out);
    write(out, checksum, in.primaryKey());
    write(out, checksum, in.attributes());
    crc8(checksum, 0); // placeholder for delete marker
    writeRowChecksum(out, checksum);
    return out;
}

std::string& write(string& out, const RowDeleteChange& in)
{
    uint8_t checksum = 0;
    writeHeader(out);
    write(out, checksum, in.primaryKey());
    writeDeleteMarker(out, checksum);
    writeRowChecksum(out, checksum);
    return out;
}

std::string& write(string& out, const RowUpdateChange& in)
{
    uint8_t checksum = 0;
    writeHeader(out);
    write(out, checksum, in.primaryKey());
    write(out, checksum, in.updates());
    crc8(checksum, 0); // placeholder for no row-delete marker
    writeRowChecksum(out, checksum);
    return out;
}

} // namespace plainbuffer
} // namespace core
} // namespace tablestore
} // namespace aliyun
