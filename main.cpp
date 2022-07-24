#include <cstdio>
#include <iostream>
#include <algorithm>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
using namespace std;
using namespace rocksdb;
const std::string PATH = "/tmp/rocksdbResult";
class Graph{
    public:
        DB* db;
        Options options;
        Graph()//构造函数
        {      //打开数据库
            options.create_if_missing = true;
            rocksdb::Status status = rocksdb::DB::Open(options, PATH, &db);
            assert(status.ok());
        }
        void closeGraph()//关闭数据库
        {
            delete db;
        }
        void add_node(std::string pk,std::string property)
        {
            std::string propertys = "";
            std::string key = "",value = "";
            int flag = 0;
            for(int i = 0; i < property.length(); i++)
            {
                if(property[i] != '=' && flag == 0) key += property[i];
                if(property[i] == '=') flag = 1;
                if(property[i] != ',' && flag == 1) value += property[i];
                if(property[i] == ',' || i == property.length()-1){
                    propertys += key + ",";
                    std::string pk_key = pk + "_" + key;
                    rocksdb::Status s = db -> Put(rocksdb::WriteOptions(),pk_key,value);
                    if(s.ok()) {
                        flag = 0;
                        key = "";
                        value = "";
                    }
                }
            }
            rocksdb::Status s = db -> Put(rocksdb::WriteOptions(),pk + "_type","node");
            propertys += "type;";
            if(s.ok())  s = db -> Put(rocksdb::WriteOptions(),pk,propertys);
        }
        void add_edge(std::string pk,std::string value)
        {

        }
        void update(std::string pk,std::string value)
        {

        }
        void delete_node(std::string pk)
        {

        }
        void delete_edge(std::string pk)
        {

        }
};


int main(){
    Graph g = Graph();
    g.add_node("wsc","name=wsc,age=15,address=china");
    std::string v;
    rocksdb::Status s = g.db -> Get(rocksdb::ReadOptions(),"wsc",&v);
    if(s.ok()) cout << "wsc:   " << v << endl;
    g.closeGraph();
}
