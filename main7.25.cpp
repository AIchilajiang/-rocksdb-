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
class Graph{
public:
    DB* db;
    Options options;
    Graph(){      //构造函数打开数据库
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, PATH, &db);
        assert(status.ok());
    }
    void closeGraph(){//关闭数据库
        delete db;
    }
    void show_all(){

    }
    void clear_all(){

    }
    void select_node(std::string pk){
        std::string propertys = "",value = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok()){
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&value);
            if(s.ok()){
                if(value != "node"){
                    cout << "数据库中不存在node:" << pk << endl;
                    return;
                }
            }
        }
        else{
            cout << "数据库中不存在node:" << pk << endl;
            return;
        }

        cout << "node:  " << pk << endl;
        std::string property = "";
        for(int i = 0; i < propertys.length(); i++){
            if(propertys[i] != ',' && propertys[i] != ';'){
                property += propertys[i];
            }
            else{
                std::string key = pk + "_" + property;
                property = "";
                value = "";
                s = db -> Get(rocksdb::ReadOptions(),key,&value);
                if(s.ok()){
                    cout << key << " = " << value << endl;
                }
                else{
                    cout << pk << "的属性" << value << "查询失败" << endl;
                }
            }
        }
    }
    void select_edge(std::string pk){
        std::string propertys = "",value = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok()){
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&value);
            if(s.ok()){
                if(value != "edge"){
                    cout << "数据库中不存在edge:" << pk << endl;
                    return;
                }
            }
        }
        else{
            cout << "数据库中不存在edge:" << pk << endl;
            return;
        }

        cout << "edge:  " << pk<< endl;
        std::string property = "";
        for(int i = 0; i < propertys.length(); i++){
            if(propertys[i] != ',' && propertys[i] != ';'){
                property += propertys[i];
            }
            else{
                std::string key = pk + "_" + property;
                property = "";
                value = "";
                s = db -> Get(rocksdb::ReadOptions(),key,&value);
                if(s.ok()){
                    cout << key << " = " << value << endl;
                }
                else{
                    cout << pk << "的属性" << value << "查询失败" << endl;
                }
            }
        }
    }
    void add_node(std::string pk,std::string property)
    {
        std::string a = "",b = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&a);
        if(s.ok() && a != "")
        {
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&b);
            if(s.ok() && b == "node")
            cout << "数据库中已经存在node:" << pk << endl;
            return ;
        }

        std::string propertys = "";
        std::string key = "",value = "";
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
        std::string a = "",b = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&a);
        if(s.ok() && a != "")
        {
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&b);
            if(s.ok() && b == "edge")
                cout << "数据库中已经存在edge:" << pk << endl;
            return ;
        }
        std::string propertys = "";
        std::string key = "",value = "";
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
        std::string a = "",b = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&a);
        if(s.ok()){
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&b);
            if(s.ok()){
                if(b != "node"){
                    cout << "数据库中不存在node:" << pk << endl;
                    return;
                }
            }
        }
        else{
            cout << "数据库中不存在node:" << pk << endl;
            return;
        }
        std::string propertys = "";
        std::string value = "";
        s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    a = pk + "_" + value;
                    s = db -> Delete(rocksdb::WriteOptions(),a);
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
        std::string a = "",b = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&a);
        if(s.ok()){
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&b);
            if(s.ok()){
                if(b != "edge"){
                    cout << "数据库中不存在edge:" << pk << endl;
                    return ;
                }
            }
        }
        else{
            cout << "数据库中不存在edge:" << pk << endl;
            return ;
        }

        std::string propertys = "";
        std::string value = "";
        s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    a = pk + "_" + value;
                    s = db -> Delete(rocksdb::WriteOptions(),a);
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
        std::string a = "",b = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&a);
        if(s.ok()){
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&b);
            if(s.ok()){
                if(b != "node"){
                    cout << "数据库中不存在node:" << pk << endl;
                    return;
                }
            }
        }
        else{
            cout << "数据库中不存在node:" << pk << endl;
            return;
        }
        std::string propertys = "";
        std::string value = "";
        s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    a = pk + "_" + value;
                    s = db -> Delete(rocksdb::WriteOptions(),a);
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
        std::string a = "",b = "";
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&a);
        if(s.ok()){
            s = db -> Get(rocksdb::ReadOptions(),pk+"_type",&b);
            if(s.ok()){
                if(b != "edge"){
                    cout << "数据库中不存在edge:" << pk << endl;
                    return ;
                }
            }
        }
        else{
            cout << "数据库中不存在edge:" << pk << endl;
            return ;
        }

        std::string propertys = "";
        std::string value = "";
        s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    a = pk + "_" + value;
                    s = db -> Delete(rocksdb::WriteOptions(),a);
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
};


int main(){
    Graph g = Graph();
    //g.add_node("wsc","name=wsc,age=15,address=china");
    //g.add_edge("win","date=22.4.14");

    g.update_node("wsc","name=wzy,age=55,address=MOON");
    g.update_edge("win","date=999,time=22.12");
    g.select_node("wsc");
    g.select_edge("win");
    //g.delete_node("wsc");
    //g.delete_edge("win");
    /*std::string v;
    rocksdb::Status s = g.db -> Get(rocksdb::ReadOptions(),"wsc",&v);
    if(s.ok()) cout << "wsc:   " << v << endl;
    s = g.db -> Get(rocksdb::ReadOptions(),"win",&v);
    if(s.ok()) cout << "win:   " << v << endl;*/
    g.closeGraph();
}