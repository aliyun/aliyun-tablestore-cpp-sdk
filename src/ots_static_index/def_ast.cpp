#include "def_ast.h"
#include "logging_assert.h"
#include "slice.h"
#include "foreach.h"
#include "jsoncpp/json/value.h"
#include <map>

using namespace ::std;
using namespace ::std::tr1;

namespace static_index {
namespace ast {

namespace {

Exceptional ExpectChar(
    const uint8_t** out,
    const uint8_t* b,
    const uint8_t* e,
    char expect)
{
    if (b >= e) {
        return Exceptional("unexpected end in pkey definition");
    }
    if (*b != expect) {
        string msg = "expect: ";
        msg.push_back(expect);
        msg.append(", real: ");
        msg.push_back(*b);
        return Exceptional(msg);
    }
    *out = b + 1;
    return Exceptional();
}

Exceptional SkipBlank(Slice* out, const Slice& in)
{
    const uint8_t* b = in.Start();
    const uint8_t* e = in.End();
    for(; b < e && IsBlank(*b); ++b) {
    }
    *out = Slice(b, e);
    return Exceptional();
}

Exceptional NextToken(Slice* token, const Slice& in)
{
    Slice skipBlank;
    OTS_TRY(SkipBlank(&skipBlank, in));
    const uint8_t* e = skipBlank.End();
    const uint8_t* tokenStart = skipBlank.Start();
    const uint8_t* tokenEnd = tokenStart;
    for(; tokenEnd < e && !IsBlank(*tokenEnd) && !IsBracket(*tokenEnd);
        ++tokenEnd) {
    }
    *token = Slice(tokenStart, tokenEnd);
    return Exceptional();
}

Exceptional NextBalancedBrackets(Slice* out, const Slice& s)
{
    const uint8_t* b = s.Start();
    const uint8_t* e = s.End();
    const uint8_t* bracketStart = b;
    OTS_TRY(ExpectChar(&b, b, e, '('));
    int64_t level = 1;
    for(; b < e && level > 0; ++b) {
        if (*b == '(') {
            ++level;
        } else if (*b == ')') {
            --level;
        }
    }
    if (level > 0) {
        return Exceptional("brackets are not balanced in pkey definition");
    }
    *out = Slice(bracketStart, b);
    return Exceptional();
}

Exceptional StrToDataType(Value::Type* out, const string& type)
{
    if (ToSlice(type) == ToSlice("Str")) {
        *out = Value::STRING;
    } else if (ToSlice(type) == ToSlice("Int")) {
        *out = Value::INTEGER;
    } else if (ToSlice(type) == ToSlice("Double")) {
        *out = Value::DOUBLE;
    } else if (ToSlice(type) == ToSlice("Bool")) {
        *out = Value::BOOLEAN;
    } else {
        return Exceptional("Unknown type: " + type);
    }
    return Exceptional();
}

class Parser
{
public:
    Logger* mLogger;
    Exceptional mExcept;
    const map<string, Value::Type, QuasiLexicographicOrderLess<string> >& mAttrTypes;
    string mTempNamePrefix;
    int64_t mCnt;

    explicit Parser(
        Logger* logger,
        const string& tableName,
        const map<string, Value::Type, QuasiLexicographicOrderLess<string> >&);

    Exceptional Parse(
        shared_ptr<Node>* root,
        const string& name, const Slice& def);

private:
    Exceptional _Parse(shared_ptr<Node>* root, const string& name, const Slice& def);
    string NewTempName();
};

Parser::Parser(
    Logger* logger,
    const string& tableName,
    const map<string, Value::Type, QuasiLexicographicOrderLess<string> >& attrs)
  : mLogger(logger),
    mAttrTypes(attrs),
    mCnt(0)
{
    mTempNamePrefix.push_back('_');
    mTempNamePrefix.append(tableName);
    mTempNamePrefix.push_back('_');
}

Exceptional Parser::Parse(
    shared_ptr<Node>* root,
    const string& name,
    const Slice& def)
{
    mTempNamePrefix.append(name);
    mTempNamePrefix.push_back('_');
    return _Parse(root, name, def);
}

Exceptional Parser::_Parse(
    shared_ptr<Node>* root,
    const string& name,
    const Slice& def)
{
    Slice x;
    OTS_TRY(SkipBlank(&x, def));
    if (x.Size() == 0) {
        return Exceptional("() in pkey definition");
    }
    if (*(x.Start()) == '(') {
        OTS_TRY(NextBalancedBrackets(&x, x));
        x = Slice(x.Start() + 1, x.End() - 1);
        Slice token;
        OTS_TRY(NextToken(&token, x));

        if (token == ToSlice("$Crc64Int")) {
            *root = shared_ptr<Node>(new Node(mLogger, name, Node::CRC64INT));
        } else if (token == ToSlice("$Crc64Str")) {
            *root = shared_ptr<Node>(new Node(mLogger, name, Node::CRC64STR));
        } else if (token == ToSlice("$Hex")) {
            *root = shared_ptr<Node>(new Node(mLogger, name, Node::HEX));
        } else if (token == ToSlice("$ShiftToUint64")) {
            *root = shared_ptr<Node>(new Node(mLogger, name, Node::SHIFT_TO_UINT64));
        } else if (token == ToSlice("|")) {
            *root = shared_ptr<Node>(new Node(mLogger, name, Node::CONCAT));
        } else {
            return Exceptional("unknown operator: " + ToString(token));
        }

        Slice rest(token.End(), x.End());
        for(; rest.Size() > 0;) {
            OTS_TRY(SkipBlank(&rest, rest));
            if (rest.Size() == 0) {
                break;
            }
            Slice nxtTree;
            if (*(rest.Start()) == '(') {
                OTS_TRY(NextBalancedBrackets(&nxtTree, rest));
            } else {
                OTS_TRY(NextToken(&nxtTree, rest));
            }
            string nxtName = NewTempName();
            shared_ptr<Node> nxtRoot;
            OTS_TRY(_Parse(&nxtRoot, nxtName, nxtTree));
            (*root)->mChildren.push_back(nxtRoot);
            
            rest = Slice(nxtTree.End(), rest.End());
        }
        if ((*root)->mNodeType != Node::CONCAT && (*root)->mChildren.size() > 1) {
            return Exceptional("more than one children on " + ToString(x));
        }
        if ((*root)->mChildren.empty()) {
            return Exceptional("no children on " + ToString(x));
        }
        switch((*root)->mNodeType) {
        case Node::ATTR: break;
        case Node::CRC64INT: {
            if ((*root)->mChildren[0]->mDataType != Value::INTEGER) {
                return Exceptional("child of $Crc64Int must be an integer.");
            }
            break;
        }
        case Node::CRC64STR: {
            if ((*root)->mChildren[0]->mDataType != Value::STRING) {
                return Exceptional("child of $Crc64Str must be a string.");
            }
            break;
        }
        case Node::HEX: {
            if ((*root)->mChildren[0]->mDataType != Value::INTEGER) {
                return Exceptional("child of $Hex must be an integer.");
            }
            break;
        }
        case Node::SHIFT_TO_UINT64: {
            if ((*root)->mChildren[0]->mDataType != Value::INTEGER) {
                return Exceptional("child of $ShiftToUint64 must be an integer.");
            }
            break;
        }
        case Node::CONCAT: {
            FOREACH_ITER(i, (*root)->mChildren) {
                if ((*i)->mDataType != Value::STRING) {
                    return Exceptional("children of | must be strings.");
                }
            }
            break;
        }
        }
    } else {
        Slice token;
        OTS_TRY(NextToken(&token, x));
        QloMap<string, Value::Type>::const_iterator it = mAttrTypes.find(token.ToString());
        if (it == mAttrTypes.end()) {
            OTS_LOG_ERROR(mLogger)
                (ToString(token))
                .What("Unknown attribute.");
            return Exceptional("Unknown attribute: " + ToString(token));
        }
        shared_ptr<Node> nd(new Node(mLogger, ToString(token), it->second));
        *root = nd;
    }
    return Exceptional();
}

string Parser::NewTempName()
{
    string res = mTempNamePrefix;
    res.append(ToString(mCnt));
    ++mCnt;
    return res;
}

Value::Type NodeTypeToDataType(Logger* logger, Node::Type tp)
{
    OTS_ASSERT(logger, tp != Node::ATTR);
    switch(tp) {
    case Node::CRC64INT: return Value::INTEGER;
    case Node::CRC64STR: return Value::INTEGER;
    case Node::HEX: return Value::STRING;
    case Node::SHIFT_TO_UINT64: return Value::INTEGER;
    case Node::CONCAT: return Value::STRING;
    default: {
        OTS_ASSERT(logger, false)((int) tp).What("unknown AST node type");
        return Value::INVALID;
    }
    }
}

Exceptional ParseAttributes(
    Logger* logger,
    deque<string>* names,
    map<string, Value::Type, QuasiLexicographicOrderLess<string> >* types,
    const Json::Value& jAttrs,
    const string& msg)
{
    if (!jAttrs.isArray()) {
        OTS_LOG_ERROR(logger)
            (ToString(jAttrs))
            .What(msg + " must be an array.");
        return Exceptional(msg + " must be an array.");
    }
    for(int i = 0, sz = jAttrs.size(); i < sz; ++i) {
        const Json::Value& jAttr = jAttrs[i];
        if (!jAttr.isObject()) {
            OTS_LOG_ERROR(logger)
                (ToString(jAttr))
                .What("Each item of " + msg + " must be an object.");
            return Exceptional("Each item of " + msg + " must be an object.");
        }

        const Json::Value& jName = jAttr["Name"];
        if (!jName.isString()) {
            OTS_LOG_ERROR(logger)
                (ToString(jName))
                .What("\"Name\" of each item of " + msg + " must be a string.");
            return Exceptional("\"Name\" of each item of " + msg + " must be a string.");
        }
        const string& name = jName.asString();

        const Json::Value& jType = jAttr["Type"];
        if (!jType.isString()) {
            OTS_LOG_ERROR(logger)
                (ToString(jType))
                .What("\"Type\" of each item of " + msg + " must be a string.");
            return Exceptional("\"Type\" of each item of " + msg + " must be a string.");
        }
        Value::Type type;
        {
            const Exceptional& ex = StrToDataType(&type, jType.asString());
            if (ex.GetCode() != Exceptional::OTS_OK) {
                OTS_LOG_ERROR(logger)
                    (ToString(jAttr))
                    .What("Unknown type");
                return ex;
            }
        }
        
        names->push_back(name);
        bool r = types->insert(make_pair(name, type)).second;
        if (!r) {
            OTS_LOG_ERROR(logger)
                (ToString(name))
                .What("Duplicated attribute name.");
            return Exceptional("Duplicated attribute name.");
        }
    }

    return Exceptional();
}

Exceptional ParseTableMeta(Logger* logger, TableMeta* meta, const Json::Value& schema)
{
    if (!schema.isObject()) {
        OTS_LOG_ERROR(logger)
            (ToString(schema))
            .What("Table schema must be an object.");
        return Exceptional("Table schema must be an object.");
    }

    const Json::Value& jTableName = schema["TableName"];
    if (!jTableName.isString()) {
        OTS_LOG_ERROR(logger)
            (ToString(schema))
            .What("\"TableName\" must be a string.");
        return Exceptional("\"TableName\" must be a string.");
    }
    meta->mTableName = jTableName.asString();

    const Json::Value& jRequired = schema["RequiredAttributes"];
    OTS_TRY(ParseAttributes(
            logger,
            &(meta->mRequiredAttributes),
            &(meta->mAttributeTypes),
            jRequired,
            "\"RequiredAttributes\""));
    
    const Json::Value& jPkey = schema["PrimaryKeys"];
    if (!jPkey.isArray()) {
        OTS_LOG_ERROR(logger)
            (ToString(schema))
            .What("\"PrimaryKeys\" must be an array.");
        return Exceptional("\"PrimaryKeys\" must be an array.");
    }
    for(int i = 0, sz = jPkey.size(); i < sz; ++i) {
        const Json::Value& jCol = jPkey[i];
        if (!jCol.isObject()) {
            OTS_LOG_ERROR(logger)
                (ToString(jCol))
                .What("Each item of \"PrimaryKeys\" must be an object.");
            return Exceptional("Each item of \"PrimaryKeys\" must be an object.");
        }

        const Json::Value& jName = jCol["Name"];
        if (!jName.isString()) {
            OTS_LOG_ERROR(logger)
                (ToString(jName))
                .What("\"Name\" of each item of \"PrimaryKeys\" must be a string.");
            return Exceptional("\"Name\" of each item of \"PrimaryKeys\" must be a string.");
        }
        const string& name = jName.asString();

        const Json::Value& jType = jCol["Type"];
        if (!jType.isString()) {
            OTS_LOG_ERROR(logger)
                (ToString(jType))
                .What("\"Type\" of each item of \"PrimaryKeys\" must be a string.");
            return Exceptional("\"Type\" of each item of \"PrimaryKeys\" must be a string.");
        }
        const string& type = jType.asString();

        if (ToSlice(type) == ToSlice("Composited")) {
            const Json::Value& jDef = jCol["Definition"];
            if (!jDef.isString()) {
                OTS_LOG_ERROR(logger)
                    (ToString(jDef))
                    .What("\"Definition\" of composited column must be a string.");
                return Exceptional("\"Definition\" of composited column must be a string.");
            }
            const string& def = jDef.asString();
            shared_ptr<ast::Node> root;
            Parser p(logger, meta->mTableName, meta->mAttributeTypes);
            OTS_TRY(p.Parse(&root, name, ToSlice(def)));
            meta->mPrimaryKey.push_back(root);
        } else {
            Value::Type tp;
            {
                const Exceptional& ex = StrToDataType(&tp, type);
                if (ex.GetCode() != Exceptional::OTS_OK) {
                    OTS_LOG_ERROR(logger)
                        (ToString(jCol))
                        .What("Unknown type");
                    return ex;
                }
            }
            QloMap<string, Value::Type>::const_iterator it = meta->mAttributeTypes.find(name);
            if (it == meta->mAttributeTypes.end()) {
                OTS_LOG_ERROR(logger)
                    (meta->mTableName)
                    (name)
                    .What("Atom column in primary key must exist in \"RequiredAttributes\" as well.");
                return Exceptional("Atom column in primary key must exist in \"RequiredAttributes\" as well.");
            }
            if (tp != it->second) {
                OTS_LOG_ERROR(logger)
                    (meta->mTableName)
                    (name)
                    .What("Type of atom column in primary key is not the same as that in \"RequiredAttributes\".");
                return Exceptional("Type of atom column in primary key is not the same as that in \"RequiredAttributes\".");
                    
            }
            shared_ptr<ast::Node> root;
            Parser p(logger, meta->mTableName, meta->mAttributeTypes);
            OTS_TRY(p.Parse(&root, name, ToSlice(name)));
            meta->mPrimaryKey.push_back(root);
        }
    }

    const Json::Value& jOptional = schema["OptionalAttributes"];
    if (!jOptional.isNull()) {
        OTS_TRY(ParseAttributes(
                logger,
                &(meta->mOptionalAttributes),
                &(meta->mAttributeTypes),
                jOptional,
                "\"OptionalAttributes\""));
    }

    return Exceptional();
}

} // namespace

void SortInBfs(deque< shared_ptr<Node> >* out, const shared_ptr<Node>& root)
{
    deque< shared_ptr<Node> > buf;
    buf.push_back(root);
    for(; !buf.empty();) {
        shared_ptr<Node> rt = buf.front();
        buf.pop_front();
        out->push_back(rt);

        FOREACH_ITER(i, rt->mChildren) {
            buf.push_back(*i);
        }
    }
}


Node::Node(
    Logger* logger,
    const string& name,
    Type tp)
  : mName(name),
    mNodeType(tp),
    mDataType(NodeTypeToDataType(logger, tp))
{}

Node::Node(
    Logger* logger,
    const string& name,
    Value::Type tp)
  : mName(name),
    mNodeType(ATTR),
    mDataType(tp)
{
    OTS_ASSERT(logger, tp != Value::INVALID);
}

} // namespace ast

Exceptional ParseSchema(Logger* logger, CollectionMeta* cmeta, const Json::Value& schema)
{
    if (!schema.isObject()) {
        OTS_LOG_ERROR(logger)
            (ToString(schema))
            .What("Collection schema must be an object.");
        return Exceptional("Collection schema must be an object.");
    }

    const Json::Value& jPriTbl = schema["PrimaryTable"];
    TableMeta priMeta;
    OTS_TRY(ast::ParseTableMeta(logger, &priMeta, jPriTbl));
    cmeta->mPrimaryTable = priMeta.mTableName;
    bool r = cmeta->mTableMetas.insert(make_pair(priMeta.mTableName, priMeta)).second;
    if (!r) {
        OTS_LOG_ERROR(logger)
            (priMeta.mTableName)
            (ToString(schema))
            .What("Duplicated table name.");
        return Exceptional("Duplicated table name: " + priMeta.mTableName);
    }

    const Json::Value& jIndexes = schema["Indexes"];
    if (!jIndexes.isNull()) {
        if (!jIndexes.isArray()) {
            OTS_LOG_ERROR(logger)
                (ToString(schema))
                .What("\"Indexes\" must be an array.");
            return Exceptional("\"Indexes\" must be an array.");
        }
        for(int i = 0, sz = jIndexes.size(); i < sz; ++i) {
            const Json::Value& jIndex = jIndexes[i];
            TableMeta indMeta;
            OTS_TRY(ast::ParseTableMeta(logger, &indMeta, jIndex));
            cmeta->mIndexes.push_back(indMeta.mTableName);
            bool r = cmeta->mTableMetas.insert(make_pair(indMeta.mTableName, indMeta)).second;
            if (!r) {
                OTS_LOG_ERROR(logger)
                    (indMeta.mTableName)
                    (ToString(schema))
                    .What("Duplicated table name.");
                return Exceptional("Duplicated table name: " + indMeta.mTableName);
            }
        }
    }
    
    return Exceptional();
}

} // namespace static_index
