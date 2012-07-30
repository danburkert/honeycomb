namespace cpp com.nearinfinity.hbase_engine
namespace java com.nearinfinity.hbase_engine

enum hbase_engine_types {
    CLOUD = 1
}

service Engine {
    void open(),

    bool createTable(1: string tableName, 2: list<string> columnFamilies),

    bool addColumnFamily(1: string tableName, 2: string familyName),

    bool removeColumnFamily(1: string tableName, 2: string familyName),

    void addData(1: string tableName, 2: map<string, map<string, map<string, string>>> data),

    bool removeTable(1: string tableName)
}
