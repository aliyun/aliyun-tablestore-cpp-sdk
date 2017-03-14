#ifndef OTS_STATIC_INDEX_DEF_AST_H
#define OTS_STATIC_INDEX_DEF_AST_H

#include "type_delegates.h"
#include "slice.h"
#include "ots_static_index/exceptional.h"
#include <tr1/memory>
#include <map>
#include <deque>
#include <string>

namespace Json {
class Value;
} // namespace Json

namespace static_index {
class Logger;

namespace ast {

struct Node
{
    enum Type
    {
        ATTR,
        CRC64INT,
        CRC64STR,
        HEX,
        SHIFT_TO_UINT64,
        CONCAT,
    };

    ::std::string mName;
    Type mNodeType;
    Value::Type mDataType;
    ::std::deque< ::std::tr1::shared_ptr<Node> > mChildren;

    explicit Node(Logger*, const ::std::string& name, Type);
    explicit Node(Logger*, const ::std::string& name, Value::Type);
};

void SortInBfs(
    ::std::deque< ::std::tr1::shared_ptr<Node> >*,
    const ::std::tr1::shared_ptr<Node>&);

} // namespace ast

struct TableMeta
{
    ::std::string mTableName;
    ::std::deque< ::std::tr1::shared_ptr<ast::Node> > mPrimaryKey;
    ::std::deque< ::std::string > mRequiredAttributes;
    ::std::deque< ::std::string > mOptionalAttributes;
    QloMap< ::std::string, Value::Type> mAttributeTypes;
};

struct CollectionMeta
{
    ::std::string mPrimaryTable;
    ::std::deque< ::std::string > mIndexes;
    QloMap< ::std::string, TableMeta> mTableMetas;
};

Exceptional ParseSchema(Logger*, CollectionMeta*, const ::Json::Value&);

} // namespace static_index

#endif /* OTS_STATIC_INDEX_DEF_AST_H */
