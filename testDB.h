#include <cstdio>
#include <iostream>
#include <algorithm>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
using namespace std;
using namespace rocksdb;
const std::string PATH = "../db_result";
void select(std::string &pk,std::string &propertys,std::string &value,DB* &db){
    std::string property = "";
    for(int i = 0; i < propertys.length(); i++){
        if(propertys[i] != ',' && propertys[i] != ';'){
            property += propertys[i];
        }
        else{
            std::string key = pk + "_" + property;
            property = "";
            value = "";
            rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),key,&value);
            if(s.ok()){
                cout << key << " = " << value << endl;
            }
            else{
                cout << pk << "的属性" << value << "查询失败" << endl;
            }
        }
    }
}
bool if_node_exist(std::string &pk,std::string &propertys,std::string &value,DB* &db){
    rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
    if(s.ok()){
        s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&value);
        if(s.ok()){
            if(value != "node"){
                cout << "数据库中不存在node:" << pk << endl;
                return false;
            }
        }
    }
    else{
        cout << "数据库中不存在node:" << pk << endl;
        return false;
    }
    return true;
}
bool if_edge_exist(std::string &pk,std::string &propertys,std::string &value,DB* &db){
    rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
    if(s.ok()){
        s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&value);
        if(s.ok()){
            if(value != "edge"){
                cout << "数据库中不存在edge:" << pk << endl;
                return false;
            }
        }
    }
    else{
        cout << "数据库中不存在edge:" << pk << endl;
        return false;
    }
    return true;
}
class Graph{
public:
    DB* db;
    Options options;
    Graph(){      //构造函数打开数据库
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, PATH, &db);
        //db -> Put(rocksdb::WriteOptions(),"all_node","");
        //db -> Put(rocksdb::WriteOptions(),"all_edge","");
        assert(status.ok());
    }
    void closeGraph(){//关闭数据库
        delete db;
    }
    void select_node(std::string pk){
        std::string propertys = "",value = "";
        bool flag = if_node_exist(pk,propertys,value,db);
        if(!flag) return;
        cout << "node:  " << pk << endl;
        select(pk,propertys,value,db);
    }
    void select_edge(std::string pk){
        std::string propertys = "",value = "";
        bool flag = if_edge_exist(pk,propertys,value,db);
        if(!flag) return;
        cout << "edge:  " << pk<< endl;
        select(pk,propertys,value,db);
    }
    void add_node(std::string pk,std::string property)
    {
        std::string propertys = "",value = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok() && propertys != "")
        {
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&value);
            if(s.ok() && value == "node")
                cout << "数据库中已经存在node:" << pk << endl;
            return ;
        }

        propertys = "";
        std::string key = "";
        value = "";
        int flag = 0;
        for(int i = 0; i < property.length(); i++)
        {
            if(property[i] != '=' && flag == 0) key += property[i];
            if(property[i] == '='){
                flag = 1;
                continue;
            }
            if(property[i] != ',' && flag == 1) value += property[i];
            if(property[i] == ',' || i == property.length()-1){
                propertys += key + ",";
                std::string pk_key = pk + "_" + key;
                s = db -> Put(rocksdb::WriteOptions(),pk_key,value);
                if(s.ok()) {
                    flag = 0;
                    key = "";
                    value = "";
                }
            }
        }
        s = db -> Put(rocksdb::WriteOptions(),pk + "_type","node");
        propertys += "type;";
        if(s.ok())  s = db -> Put(rocksdb::WriteOptions(),pk,propertys);
    }
    void add_edge(std::string pk,std::string property)
    {
        std::string propertys = "",value = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok() && propertys != "")
        {
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&value);
            if(s.ok() && value == "edge")
                cout << "数据库中已经存在edge:" << pk << endl;
            return ;
        }
        propertys = "";
        std::string key = "";
        value = "";
        int flag = 0;
        for(int i = 0; i < property.length(); i++)
        {
            if(property[i] != '=' && flag == 0) key += property[i];
            if(property[i] == '='){
                flag = 1;
                continue;
            }
            if(property[i] != ',' && flag == 1) value += property[i];
            if(property[i] == ',' || i == property.length()-1){
                propertys += key + ",";
                std::string pk_key = pk + "_" + key;
                s = db -> Put(rocksdb::WriteOptions(),pk_key,value);
                if(s.ok()) {
                    flag = 0;
                    key = "";
                    value = "";
                }
            }
        }
        s = db -> Put(rocksdb::WriteOptions(),pk + "_type","edge");
        propertys += "type;";
        if(s.ok())  s = db -> Put(rocksdb::WriteOptions(),pk,propertys);
    }
    void delete_node(std::string pk)
    {
        std::string propertys = "";
        std::string value = "";
        bool flag = if_node_exist(pk,propertys,value,db);
        if(!flag) return;
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    std::string node_to_delete = pk + "_" + value;
                    s = db -> Delete(rocksdb::WriteOptions(),node_to_delete);
                    if(s.ok()){
                        value = "";
                    }
                }
            }
            s = db -> Delete(rocksdb::WriteOptions(),pk);
        }
    }
    void delete_edge(std::string pk)
    {
        std::string propertys = "",value = "";
        bool flag = if_edge_exist(pk,propertys,value,db);
        if(!flag) return;
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    std::string edge_to_delete = pk + "_" + value;
                    s = db -> Delete(rocksdb::WriteOptions(),edge_to_delete);
                    if(s.ok()){
                        value = "";
                    }
                }
            }
            s = db -> Delete(rocksdb::WriteOptions(),pk);
        }
    }
    void update_node(std::string pk,std::string property)
    {
        std::string propertys = "",value = "";
        bool f = if_node_exist(pk,propertys,value,db);
        if(!f) return;
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    std::string node_to_update = pk + "_" + value;
                    s = db -> Delete(rocksdb::WriteOptions(),node_to_update);
                    if(s.ok()){
                        value = "";
                    }
                }
            }
            s = db -> Delete(rocksdb::WriteOptions(),pk);
        }
        propertys = "";
        std::string key = "";
        value = "";
        int flag = 0;
        for(int i = 0; i < property.length(); i++)
        {
            if(property[i] != '=' && flag == 0) key += property[i];
            if(property[i] == '='){
                flag = 1;
                continue;
            }
            if(property[i] != ',' && flag == 1) value += property[i];
            if(property[i] == ',' || i == property.length()-1){
                propertys += key + ",";
                std::string pk_key = pk + "_" + key;
                s = db -> Put(rocksdb::WriteOptions(),pk_key,value);
                if(s.ok()) {
                    flag = 0;
                    key = "";
                    value = "";
                }
            }
        }
        s = db -> Put(rocksdb::WriteOptions(),pk + "_type","node");
        propertys += "type;";
        if(s.ok())  s = db -> Put(rocksdb::WriteOptions(),pk,propertys);
    }
    void update_edge(std::string pk,std::string property)
    {
        std::string propertys = "",value = "";
        bool f = if_edge_exist(pk,propertys,value,db);
        if(!f) return;
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    std::string edge_to_update = pk + "_" + value;
                    s = db -> Delete(rocksdb::WriteOptions(),edge_to_update);
                    if(s.ok()){
                        value = "";
                    }
                }
            }
            s = db -> Delete(rocksdb::WriteOptions(),pk);
        }
        propertys = "";
        std::string key = "";
        value = "";
        int flag = 0;
        for(int i = 0; i < property.length(); i++)
        {
            if(property[i] != '=' && flag == 0) key += property[i];
            if(property[i] == '='){
                flag = 1;
                continue;
            }
            if(property[i] != ',' && flag == 1) value += property[i];
            if(property[i] == ',' || i == property.length()-1){
                propertys += key + ",";
                std::string pk_key = pk + "_" + key;
                s = db -> Put(rocksdb::WriteOptions(),pk_key,value);
                if(s.ok()) {
                    flag = 0;
                    key = "";
                    value = "";
                }
            }
        }
        s = db -> Put(rocksdb::WriteOptions(),pk + "_type","edge");
        propertys += "type;";
        if(s.ok())  s = db -> Put(rocksdb::WriteOptions(),pk,propertys);
    }
    void show_all_node(){
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
        int num = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string key = it -> key().ToString();
            std::string value = it -> value().ToString();
            int L = value.length();
            if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
            std::string type;
            db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
            if(type != "node") continue;
            std::string propertys = "";
            value = "";
            if(num == 0)    cout << "node:  " << key;
            else    cout << "  " << key;
            num++;
        }
        cout << endl;
        if(num > 1)     cout << "there is " << num << " nodes" << endl;
        else    cout << "there is " << num << " node" << endl;
    }
    void show_all_edge(){
        int num = 0;
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string key = it -> key().ToString();
            std::string value = it -> value().ToString();
            int L = value.length();
            if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
            std::string type;
            db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
            if(type != "edge") continue;
            std::string propertys = "";
            value = "";
            if(num == 0)    cout << "edge:  " << key;
            else    cout << "  " << key;
            num++;
        }
        cout << endl;
        if(num > 1)     cout << "there is " << num << " edges" << endl;
        else    cout << "there is " << num << " edge" << endl;
    }
    void clear_all(){
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string pk = it -> key().ToString();
            rocksdb::Status s = db -> Delete(rocksdb::WriteOptions(),pk);
        }
        cout << "successfully clear_all" << endl;
    }
};
