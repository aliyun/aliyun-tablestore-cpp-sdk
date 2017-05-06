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
#include "security.hpp"
#include "tablestore/util/mempiece.hpp"
#include "tablestore/util/assert.hpp"
#include <cstring>
extern "C" {
#include <openssl/bio.h> 
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
}

using namespace std;

namespace aliyun {
namespace tablestore {
namespace util {

string md5(const deque<MemPiece>& xs)
{
    Md5 md;
    FOREACH_ITER(i, xs) {
        md.update(*i);
    }
    uint8_t digest[Md5::kLength];
    md.finalize(MutableMemPiece(digest, Md5::kLength));
    Base64Encoder b64;
    b64.update(MemPiece::from(digest));
    b64.finalize();
    return b64.base64().to<string>();
}

class Md5Ctx
{
public:
    explicit Md5Ctx()
    {
        int r = MD5_Init(&mCtx);
        OTS_ASSERT(r)
            .what("Security: fail to initialize openssl MD5.");
    }

    MD5_CTX* get()
    {
        return &mCtx;
    }

private:
    MD5_CTX mCtx;
};

Md5::Md5()
  : mFinalized(false)
{
    mCtx.reset(new Md5Ctx());
}

Md5::~Md5()
{
    OTS_ASSERT(mFinalized)
        .what("Security: destroying a MD5 which is not finalized.");
}

void Md5::update(const MemPiece& p)
{
    OTS_ASSERT(!mFinalized)
        .what("Security: it is forbidden to update a finalized MD5.");
    int r = MD5_Update(mCtx->get(), p.data(), p.length());
    OTS_ASSERT(r)
        .what("Security: fail to update openssl MD5.");
}

void Md5::finalize(const MutableMemPiece& p)
{
    OTS_ASSERT(!mFinalized)
        .what("Security: it is forbidden to finalize a finalized MD5.");
    const int64_t kLength = Md5::kLength;
    OTS_ASSERT(p.length() >= kLength)
        (p.length())
        (kLength)
        .what("Security: MD5 requires at least 16 bytes to store digest.");
    int r = MD5_Final(p.begin(), mCtx->get());
    OTS_ASSERT(r)
        .what("Security: fail to finalize MD5.");
    mFinalized = true;
}


class Sha1Ctx
{
public:
    explicit Sha1Ctx()
    {
        int r = SHA1_Init(&mCtx);
        OTS_ASSERT(r)
            .what("Security: fail to initialize SHA1.");
    }

    SHA_CTX* get()
    {
        return &mCtx;
    }

private:
    SHA_CTX mCtx;
};

Sha1::Sha1()
  : mFinalized(false)
{
    mCtx.reset(new Sha1Ctx());
}

Sha1::~Sha1()
{
    OTS_ASSERT(mFinalized)
        .what("Security: destroying a MD5 which is not finalized.");
}

void Sha1::update(const MemPiece& p)
{
    OTS_ASSERT(!mFinalized)
        .what("Security: it is forbidden to update a finalized MD5.");
    int r = SHA1_Update(mCtx->get(), p.data(), p.length());
    OTS_ASSERT(r)
        .what("Security: fail to update openssl MD5.");
}

void Sha1::finalize(const MutableMemPiece& p)
{
    OTS_ASSERT(!mFinalized)
        .what("Security: it is forbidden to finalize a finalized MD5.");
    static const int64_t kLength = Sha1::kLength;
    OTS_ASSERT(p.length() >= kLength)
        (p.length())
        (kLength)
        .what("Security: SHA1 requires at least 20 bytes to store digest.");
    int r = SHA1_Final(p.begin(), mCtx->get());
    OTS_ASSERT(r)
        .what("Security: fail to finalize MD5.");
    mFinalized = true;
}


class Base64Ctx
{
public:
    explicit Base64Ctx()
      : mB64(NULL)
    {
        mB64 = BIO_new(BIO_f_base64());
        BIO_set_flags(mB64, BIO_FLAGS_BASE64_NO_NL);
        BIO* bmem = BIO_new(BIO_s_mem());
        mB64 = BIO_push(mB64, bmem);
    }

    ~Base64Ctx()
    {
        BIO_free_all(mB64);
    }

    BIO* get()
    {
        return mB64;
    }

private:
    BIO* mB64;
};

Base64Encoder::Base64Encoder()
  : mCtx(new Base64Ctx()),
    mFinalized(false)
{}

Base64Encoder::~Base64Encoder()
{
}

void Base64Encoder::update(const MemPiece& p)
{
    OTS_ASSERT(!mFinalized)
        .what("Security: it is forbidden to update a finalized BASE64.");
    int ret = BIO_write(mCtx->get(), (char*) p.data(), p.length());
    OTS_ASSERT(ret >= 0)
        (ret);
}

void Base64Encoder::finalize()
{
    OTS_ASSERT(!mFinalized)
        .what("Security: it is forbidden to finalize a BASE64 twice.");
    mFinalized = true;
    int ret = BIO_flush(mCtx->get());
    OTS_ASSERT(ret >= 0)
        (ret);
}

MemPiece Base64Encoder::base64() const
{
    OTS_ASSERT(mFinalized)
        .what("Security: it is forbidden to fetch result from an unfinalized BASE64.");
    BUF_MEM* bptr;
    BIO_get_mem_ptr(mCtx->get(), &bptr);
    return MemPiece(bptr->data, bptr->length);
}


HmacSha1::HmacSha1(const MemPiece& key)
  : mFinalized(false)
{
    uint8_t ipad[64];
    uint8_t opad[64];
    memset(ipad, 0x36, sizeof(ipad));
    memset(opad, 0x5c, sizeof(opad));
    if (key.length() <= 64) {
        const uint8_t* b = key.data();
        const uint8_t* e = b + key.length();
        uint8_t* i = ipad;
        uint8_t* o = opad;
        for(; b < e; ++b, ++i, ++o) {
            *i ^= *b;
            *o ^= *b;
        }
    } else {
        Sha1 shrink;
        shrink.update(key);
        uint8_t shrinkedKey[Sha1::kLength];
        shrink.finalize(MutableMemPiece(shrinkedKey, Sha1::kLength));
        const uint8_t* b = shrinkedKey;
        const uint8_t* e = b + Sha1::kLength;
        uint8_t* i = ipad;
        uint8_t* o = opad;
        for(; b < e; ++b, ++i, ++o) {
            *i ^= *b;
            *o ^= *b;
        }
    }
    mPass1.update(MemPiece::from(ipad));
    mPass2.update(MemPiece::from(opad));
}

HmacSha1::~HmacSha1()
{
    OTS_ASSERT(mFinalized)
        .what("Security: it is forbidden to destroying a HmacSha1 which is unfinalized.");
}

void HmacSha1::update(const MemPiece& p)
{
    OTS_ASSERT(!mFinalized)
        .what("Security: it is forbidden to update a finalized HmacSha1.");
    mPass1.update(p);
}

void HmacSha1::finalize(const MutableMemPiece& p)
{
    OTS_ASSERT(!mFinalized)
        .what("Security: it is forbidden to finalize a HmacSha1 twice.");
    mFinalized = true;
    uint8_t md[Sha1::kLength];
    mPass1.finalize(MutableMemPiece(md, Sha1::kLength));
    mPass2.update(MemPiece::from(md));
    mPass2.finalize(p);
}

} // namespace util
} // namespace tablestore
} // namespace aliyun
