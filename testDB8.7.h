#include <cstdio>
#include <iostream>
#include <algorithm>
#include <queue>
#include <map>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
using namespace std;
using namespace rocksdb;
void show(std::string &pk,std::string &propertys,std::string &value,DB* &db){
    std::string property = "";
    for(int i = 0; i < propertys.length(); i++){
        if(propertys[i] != ',' && propertys[i] != ';'){
            property += propertys[i];
        }
        else{
            std::string key = pk + "_" + property;
            //cout << key << endl;
            property = "";
            value = "";
            rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),key,&value);
            if(s.ok()){
                cout << key << " = " << value << endl;
            }
            else{
                cout << pk << "的属性:" << key << "查询失败" << endl;
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
    DB* db;//存储node、edge的属性
    DB* relation;//存储图的拓扑结构
    Options options;
    Graph(){      //构造函数打开数据库
        options.create_if_missing = true;
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        options.enable_pipelined_write = true;//开启流水线写入方式
        options.enable_write_thread_adaptive_yield = true;
        options.allow_concurrent_memtable_write = true;//开启后允许并发写memtable
        rocksdb::Status status = rocksdb::DB::Open(options, "../db_result", &db);
        assert(status.ok());
        status = rocksdb::DB::Open(options, "../relation_result", &relation);
        assert(status.ok());

        std::string node_num = "";
        rocksdb::Status s = db -> Get(rocksdb:: ReadOptions(),"node_num",&node_num);
        if(!s.ok()){
            db -> Put(rocksdb::WriteOptions(),"node_num","0");
        }
        std::string edge_num = "";
        s = db -> Get(rocksdb:: ReadOptions(),"edge_num",&node_num);
        if(!s.ok()){
            db -> Put(rocksdb::WriteOptions(),"edge_num","0");
        }
    }
    void closeGraph(){//关闭数据库
        delete db;
        delete relation;
    }
    void show_node(std::string pk){//由pk来查询node的各项属性值
        std::string propertys = "",value = "";
        bool flag = if_node_exist(pk,propertys,value,db);
        if(!flag) return;
        cout << "node:  " << pk << endl;
        show(pk,propertys,value,db);
    }
    void show_edge(std::string pk){//由pk来查询edge的各项属性值
        std::string propertys = "",value = "";
        bool flag = if_edge_exist(pk,propertys,value,db);
        if(!flag) return;
        cout << "edge:  " << pk<< endl;
        show(pk,propertys,value,db);
    }
    void select_node(std::string conditions){
        //首先处理多个用，分开的条件
        int cnum = 0;
        std::string condition[50];
        for(int i = 0; i < conditions.length(); i++){
            if(conditions[i] != ',')
                condition[cnum] += conditions[i];
            else cnum++;//最终为0~j
        }
        std::string a[50],op[50],b[50];
        for(int i = 0; i <= cnum; i++) {
            //单个查询条件的结构为a  op  b
            int flag = 0;
            for (int j = 0; j < condition[i].length(); j++) {
                if (condition[i][j] != '=' && condition[i][j] != '>' && condition[i][j] != '<' && flag == 0) {
                    a[i] += condition[i][j];
                } else if (flag == 0) {
                    flag = 1;
                    op[i] += condition[i][j];
                    if (condition[i][j + 1] == '=') {
                        op[i] += '=';
                        j++;
                    }
                    //cout << op << endl;
                }
                if (condition[i][j] != '=' && condition[i][j] != '>' && condition[i][j] != '<' && flag == 1) {
                    b[i] += condition[i][j];
                }
            }//提取到单个查询条件 a op b
        }
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
        int num = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()){
            std::string key = it -> key().ToString();
            std::string value = it -> value().ToString();
            int L = value.length();
            if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
            std::string type;
            db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
            if(type != "node") continue;
            int F = 0;//表示当前pk符合条件
            for(int i = 0; i <= cnum; i++){
                std::string c = key + "_" + a[i];//要判断的属性
                std::string d = "";//c所对应的属性值

                db -> Get(rocksdb::ReadOptions(),c,&d);
                //cout << d << endl;
                //cout << op << endl;
                //cout << b << endl;
                if(op[i] == "="){
                    if(d != b[i]) F = 1;//不符合查询条件
                }
                else if(op[i] == ">"){
                    if(b[i].length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d <= b[i]) F = 1;
                    }
                    else if(b[i].length() > d.length()){
                        F = 1;
                    }
                }
                else if(op[i] == "<"){
                    if(b[i].length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d >= b[i]) F = 1;
                    }
                    else if(b[i].length() < d.length()){
                        F = 1;
                    }
                }
                else if(op[i] == ">="){
                    if(b[i].length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d < b[i])F = 1;
                    }
                    else if(b[i].length() > d.length()){
                        F = 1;
                    }
                }
                else if(op[i] == "<="){
                    if(b[i].length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d > b[i]) F = 1;
                    }
                    else if(b[i].length() < d.length()){
                        F = 1;
                    }
                }
                if(F == 1) break;
            }
            if(F == 0){
                num++;
                if(num == 1)    cout << "node:  " << key;
                else    cout << "  " << key;
            }
        }
        cout << endl;
        if(num > 1)     cout << "there are " << num << " nodes" << endl;
        else    cout << "there is " << num << " node" << endl;
    }
    void select_edge(std::string conditions){
        //首先处理多个用，分开的条件
        int cnum = 0;
        std::string condition[50];
        for(int i = 0; i < conditions.length(); i++){
            if(conditions[i] != ',')
                condition[cnum] += conditions[i];
            else cnum++;//最终为0~j
        }
        std::string a[50],op[50],b[50];
        for(int i = 0; i <= cnum; i++) {
            //单个查询条件的结构为a  op  b
            int flag = 0;
            for (int j = 0; j < condition[i].length(); j++) {
                if (condition[i][j] != '=' && condition[i][j] != '>' && condition[i][j] != '<' && flag == 0) {
                    a[i] += condition[i][j];
                } else if (flag == 0) {
                    flag = 1;
                    op[i] += condition[i][j];
                    if (condition[i][j + 1] == '=') {
                        op[i] += '=';
                        j++;
                    }
                    //cout << op << endl;
                }
                if (condition[i][j] != '=' && condition[i][j] != '>' && condition[i][j] != '<' && flag == 1) {
                    b[i] += condition[i][j];
                }
            }//提取到单个查询条件 a op b
        }
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
        int num = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()){
            std::string key = it -> key().ToString();
            std::string value = it -> value().ToString();
            int L = value.length();
            if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
            std::string type;
            db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
            if(type != "edge") continue;
            int F = 0;//表示当前pk符合条件
            for(int i = 0; i <= cnum; i++){
                std::string c = key + "_" + a[i];//要判断的属性
                std::string d = "";//c所对应的属性值
                db -> Get(rocksdb::ReadOptions(),c,&d);
                //cout << d << endl;
                //cout << op << endl;
                //cout << b << endl;
                if(op[i] == "="){
                    if(d != b[i]) F = 1;//不符合查询条件
                }
                else if(op[i] == ">"){
                    if(b[i].length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d <= b[i]) F = 1;
                    }
                    else if(b[i].length() > d.length()){
                        F = 1;
                    }
                }
                else if(op[i] == "<"){
                    if(b[i].length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d >= b[i]) F = 1;
                    }
                    else if(b[i].length() < d.length()){
                        F = 1;
                    }
                }
                else if(op[i] == ">="){
                    if(b[i].length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d < b[i])F = 1;
                    }
                    else if(b[i].length() > d.length()){
                        F = 1;
                    }
                }
                else if(op[i] == "<="){
                    if(b[i].length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d > b[i]) F = 1;
                    }
                    else if(b[i].length() < d.length()){
                        F = 1;
                    }
                }
                if(F == 1) break;
            }
            if(F == 0){
                num++;
                if(num == 1)    cout << "node:  " << key;
                else    cout << "  " << key;
            }
        }
        cout << endl;
        if(num > 1)     cout << "there are " << num << " edges" << endl;
        else    cout << "there is " << num << " edge" << endl;
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
        rocksdb::WriteBatch batch;
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
                batch.Put(pk_key,value);
                //s = db -> Put(rocksdb::WriteOptions(),pk_key,value);
                flag = 0;
                key = "";
                value = "";
            }
        }
        batch.Put(pk + "_type","node");
        //s = db -> Put(rocksdb::WriteOptions(),pk + "_type","node");
        propertys += "type;";
        batch.Put(pk,propertys);
        //db -> Put(rocksdb::WriteOptions(),pk,propertys);


        std::string node_num = "";
        db -> Get(rocksdb:: ReadOptions(),"node_num",&node_num);
        int num = atoi(node_num.c_str()) + 1;
        batch.Put("node_num",to_string(num));
        db->Write(rocksdb::WriteOptions(), &batch);
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
        std::string start = "",end = "";
        int F = 0;
        for(int i = 0; i < pk.length(); i++){
            if(F == 0 && pk[i] == '_'){
                F = 1;
            }
            else if(F == 1 && pk[i] == '_'){
                F = 2;
            }
            if(F == 1 && pk[i] != '_'){
                start += pk[i];
            }
            if(F == 2 && pk[i] != '_'){
                end += pk[i];
            }
        }
        //添加边的时候，应当先判断start和end两个node是否存在
        propertys = "";
        value = "";
        bool start_exist = if_node_exist(start,propertys,value,db);
        propertys = "";
        value = "";
        bool end_exist = if_node_exist(end,propertys,value,db);
        if(start_exist && end_exist != true) return ;


        std::string old_relation = "";
        //nodename_start,存储的是，nodename作为起点的边
        relation -> Get(rocksdb::ReadOptions(),start+"_start",&old_relation);
        std::string new_relation = old_relation +  pk + ",";

        //relation -> Delete(rocksdb::WriteOptions(),start+"_start");
        relation -> Put(rocksdb::WriteOptions(),start+"_start",new_relation);
        //更新拓扑结构，在pk_start里添加终点

        old_relation = "";
        relation -> Get(rocksdb::ReadOptions(),end+"_end",&old_relation);
        new_relation = old_relation +  pk + ",";

        //relation -> Delete(rocksdb::WriteOptions(),end+"_end");
        relation -> Put(rocksdb::WriteOptions(),end+"_end",new_relation);
        //在pk_end添加起点
        //cout << new_relation << endl;


        propertys = "";
        std::string key = "";
        value = "";
        int flag = 0;
        rocksdb::WriteBatch batch;
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
                batch.Put(pk_key,value);
                //s = db -> Put(rocksdb::WriteOptions(),pk_key,value);
                if(s.ok()) {
                    flag = 0;
                    key = "";
                    value = "";
                }
            }
        }
        batch.Put(pk + "_type","edge");
        batch.Put(pk + "_start",start);
        batch.Put(pk + "_end",end);
        propertys += "start,end,type;";
        batch.Put(pk,propertys);
        //s = db -> Put(rocksdb::WriteOptions(),pk + "_type","edge");
        //db -> Put(rocksdb::WriteOptions(),pk + "_start",start);
        //db -> Put(rocksdb::WriteOptions(),pk + "_end",end);
        //if(s.ok())  s = db -> Put(rocksdb::WriteOptions(),pk,propertys);
        std::string edge_num;
        db -> Get(rocksdb:: ReadOptions(),"edge_num",&edge_num);
        int num = atoi(edge_num.c_str()) + 1;
        //db -> Put(rocksdb::WriteOptions(),"edge_num",to_string(num));
        batch.Put("edge_num",to_string(num));
        db->Write(rocksdb::WriteOptions(), &batch);
    }
    void delete_node(std::string pk)
    {
        std::string propertys = "";
        std::string value = "";
        bool flag = if_node_exist(pk,propertys,value,db);
        if(!flag) return;
        //删除node时，应该将与其相连的edge也删除
        std::string pk_start = "",pk_end = "";
        relation -> Get(rocksdb::ReadOptions(),pk + "_start",&pk_start);
        relation -> Get(rocksdb::ReadOptions(),pk + "_end",&pk_end);
        std::string a = "";
        for(int i = 0; i < pk_start.length();i++){
            if(pk_start[i] != ',') a += pk_start[i];
            else{
                delete_edge(a);
                a = "";
            }
        }
        for(int i = 0; i < pk_end.length();i++){
            if(pk_end[i] != ',') a += pk_end[i];
            else{
                delete_edge(a);
                a = "";
            }
        }
        relation -> Delete(rocksdb::WriteOptions(),pk + "_start");
        relation -> Delete(rocksdb::WriteOptions(),pk + "_end");



        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        rocksdb::WriteBatch batch;
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    std::string node_to_delete = pk + "_" + value;
                    batch.Delete(node_to_delete);
                    //s = db -> Delete(rocksdb::WriteOptions(),node_to_delete);
                    if(s.ok()){
                        value = "";
                    }
                }
            }
            batch.Delete(pk);

            //s = db -> Delete(rocksdb::WriteOptions(),pk);
            std::string node_num;
            db -> Get(rocksdb:: ReadOptions(),"node_num",&node_num);
            int num = atoi(node_num.c_str()) - 1;
            //db -> Put(rocksdb::WriteOptions(),"node_num",to_string(num));
            batch.Put("node_num",to_string(num));
            db->Write(rocksdb::WriteOptions(), &batch);
        }
    }
    void delete_edge(std::string pk)
    {
        std::string propertys = "",value = "";
        bool flag = if_edge_exist(pk,propertys,value,db);
        if(!flag) return;
        std::string start = "",end = "";
        int F = 0;
        for(int i = 0; i < pk.length(); i++){
            if(F == 0 && pk[i] == '_'){
                F = 1;
            }
            else if(F == 1 && pk[i] == '_'){
                F = 2;
            }
            if(F == 1 && pk[i] != '_'){
                start += pk[i];
            }
            if(F == 2 && pk[i] != '_'){
                end += pk[i];
            }
        }
        std::string old_relation = "";
        relation -> Get(rocksdb::ReadOptions(),start+"_start",&old_relation);
        std::string new_relation = "";
        std::string End = "";
        for(int i = 0; i < old_relation.length(); i++){
            if(old_relation[i] != ',') End += old_relation[i];
            else{
                if(End != pk){
                    new_relation += End + ",";
                    End = "";
                }
            }
        }
        rocksdb::WriteBatch batch_relation;
        batch_relation.Delete(start+"_start");
        batch_relation.Put(start+"_start",new_relation);
        //relation -> Delete(rocksdb::WriteOptions(),start+"_start");
        //relation -> Put(rocksdb::WriteOptions(),start+"_start",new_relation);
        old_relation = "";
        relation -> Get(rocksdb::ReadOptions(),end+"_end",&old_relation);
        new_relation = "";
        std::string Start = "";
        for(int i = 0; i < old_relation.length(); i++){
            if(old_relation[i] != ',') Start += old_relation[i];
            else{
                if(Start != pk){
                    new_relation += Start + ",";
                    Start = "";
                }
            }
        }
        //relation -> Delete(rocksdb::WriteOptions(),end+"_end");
        //relation -> Put(rocksdb::WriteOptions(),end+"_end",new_relation);
        batch_relation.Delete(end+"_end");
        batch_relation.Put(end+"_end",new_relation);
        relation ->Write(rocksdb::WriteOptions(), &batch_relation);
        //cout << new_relation << endl;
        rocksdb::WriteBatch batch_db;
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
                    batch_db.Delete(edge_to_delete);
                    //s = db -> Delete(rocksdb::WriteOptions(),edge_to_delete);
                    value = "";
                }
            }
            batch_db.Delete(pk);

            //s = db -> Delete(rocksdb::WriteOptions(),pk);
            std::string edge_num;
            db -> Get(rocksdb:: ReadOptions(),"edge_num",&edge_num);
            int num = atoi(edge_num.c_str()) + 1;
            //db -> Put(rocksdb::WriteOptions(),"edge_num",to_string(num));
            batch_db.Put("edge_num",to_string(num));
            db -> Write(rocksdb::WriteOptions(),&batch_db);
        }
    }
    void update_node(std::string pk,std::string property)
    {
        std::string propertys = "",value = "";
        bool f = if_node_exist(pk,propertys,value,db);
        if(!f) return;
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        rocksdb::WriteBatch batch;
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    std::string node_to_update = pk + "_" + value;
                    batch.Delete(node_to_update);
                    //s = db -> Delete(rocksdb::WriteOptions(),node_to_update);
                    value = "";

                }
            }
            batch.Delete(pk);
            //s = db -> Delete(rocksdb::WriteOptions(),pk);
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
                batch.Put(pk_key,value);
                //s = db -> Put(rocksdb::WriteOptions(),pk_key,value);
                flag = 0;
                key = "";
                value = "";

            }
        }
        batch.Put(pk + "_type","node");
        //s = db -> Put(rocksdb::WriteOptions(),pk + "_type","node");
        propertys += "type;";
        //if(s.ok())  s = db -> Put(rocksdb::WriteOptions(),pk,propertys);
        batch.Put(pk,propertys);
        db -> Write(rocksdb::WriteOptions(),&batch);
    }
    void update_edge(std::string pk,std::string property)
    {
        std::string propertys = "",value = "";
        bool f = if_edge_exist(pk,propertys,value,db);
        if(!f) return;
        rocksdb::Status s = db -> Get(rocksdb::ReadOptions(),pk,&propertys);
        rocksdb::WriteBatch batch;
        if(s.ok())
        {
            int L = propertys.length();
            for(int i = 0; i < L; i++){
                if(propertys[i] != ',' && propertys[i] != ';'){
                    value += propertys[i];
                }
                else{
                    if(value != "start" && value != "end"){
                        std::string edge_to_update = pk + "_" + value;
                        batch.Delete(edge_to_update);
                    }
                    //s = db -> Delete(rocksdb::WriteOptions(),edge_to_update);
                    value = "";

                }
            }
            batch.Delete(pk);
            //s = db -> Delete(rocksdb::WriteOptions(),pk);
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
                batch.Put(pk_key,value);
                //s = db -> Put(rocksdb::WriteOptions(),pk_key,value);
                flag = 0;
                key = "";
                value = "";

            }
        }
        batch.Put(pk + "_type","edge");
        propertys += "start,end,type;";
        //s = db -> Put(rocksdb::WriteOptions(),pk + "_type","edge");
        //if(s.ok())  s = db -> Put(rocksdb::WriteOptions(),pk,propertys);
        batch.Put(pk,propertys);
        db ->Write(rocksdb::WriteOptions(),&batch);
    }
    void all_node(){
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
            if(num == 0)    cout << "node:  " << key;
            else    cout << "  " << key;
            num++;
        }
        cout << endl;
        if(num > 1)     cout << "there are " << num << " nodes" << endl;
        else    cout << "there is " << num << " node" << endl;
    }
    void all_edge(){
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
            if(num == 0)    cout << "edge:  " << key;
            else    cout << "  " << key;
            num++;
        }
        cout << endl;
        if(num > 1)     cout << "there are " << num << " edges" << endl;
        else    cout << "there is " << num << " edge" << endl;
    }
    void clear_all(){
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string pk = it -> key().ToString();
            rocksdb::Status s = db -> Delete(rocksdb::WriteOptions(),pk);
        }
        it = relation->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string pk = it -> key().ToString();
            rocksdb::Status s = relation -> Delete(rocksdb::WriteOptions(),pk);
        }
        cout << "successfully clear_all" << endl;
    }
};

