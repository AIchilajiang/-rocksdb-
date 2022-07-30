#include <cstdio>
#include <iostream>
#include <algorithm>
#include <queue>
#include <map>
#include <cstring>
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
    DB* db;//存储node、edge的属性
    DB* relation;//存储图的拓扑结构
    Options options;
    Graph(){      //构造函数打开数据库
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, "../db_result", &db);
        assert(status.ok());
        status = rocksdb::DB::Open(options, "../relation_result", &relation);
        assert(status.ok());
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
                std::string a = "", op = "",b = "";
                //单个查询条件的结构为a  op  b
                int flag = 0;
                for(int j = 0; j < condition[i].length();j++){
                    if(condition[i][j] != '=' && condition[i][j] != '>' && condition[i][j] != '<' && flag == 0){
                        a += condition[i][j];
                    }
                    else if(flag == 0){
                        flag = 1;
                        op += condition[i][j];
                        if(condition[i][j+1] == '='){
                            op += '=';
                            j++;
                        }
                        //cout << op << endl;
                    }
                    if(condition[i][j] != '=' && condition[i][j] != '>' && condition[i][j] != '<' && flag == 1){
                        b += condition[i][j];
                    }
                }//提取到单个查询条件 a op b
                std::string c = key + "_" + a;//要判断的属性
                std::string d = "";//c所对应的属性值

                db -> Get(rocksdb::ReadOptions(),c,&d);
                //cout << d << endl;
                //cout << op << endl;
                //cout << b << endl;
                if(op == "="){
                    if(d != b) F = 1;//不符合查询条件
                }
                else if(op == ">"){
                    if(b.length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d <= b) F = 1;
                    }
                    else if(b.length() > d.length()){
                        F = 1;
                    }
                }
                else if(op == "<"){
                    if(b.length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d >= b) F = 1;
                    }
                    else if(b.length() < d.length()){
                        F = 1;
                    }
                }
                else if(op == ">="){
                    if(b.length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d < b)F = 1;
                    }
                    else if(b.length() > d.length()){
                        F = 1;
                    }
                }
                else if(op == "<="){
                    if(b.length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d > b) F = 1;
                    }
                    else if(b.length() < d.length()){
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
                std::string a = "", op = "",b = "";
                //单个查询条件的结构为a  op  b
                int flag = 0;
                for(int j = 0; j < condition[i].length();j++){
                    if(condition[i][j] != '=' && condition[i][j] != '>' && condition[i][j] != '<' && flag == 0){
                        a += condition[i][j];
                    }
                    else if(flag == 0){
                        flag = 1;
                        op += condition[i][j];
                        if(condition[i][j+1] == '='){
                            op += '=';
                            j++;
                        }
                        //cout << op << endl;
                    }
                    if(condition[i][j] != '=' && condition[i][j] != '>' && condition[i][j] != '<' && flag == 1){
                        b += condition[i][j];
                    }
                }//提取到单个查询条件 a op b
                std::string c = key + "_" + a;//要判断的属性
                std::string d = "";//c所对应的属性值

                db -> Get(rocksdb::ReadOptions(),c,&d);
                //cout << d << endl;
                //cout << op << endl;
                //cout << b << endl;
                if(op == "="){
                    if(d != b) F = 1;//不符合查询条件
                }
                else if(op == ">"){
                    if(b.length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d <= b) F = 1;
                    }
                    else if(b.length() > d.length()){
                        F = 1;
                    }
                }
                else if(op == "<"){
                    if(b.length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d >= b) F = 1;
                    }
                    else if(b.length() < d.length()){
                        F = 1;
                    }
                }
                else if(op == ">="){
                    if(b.length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d < b)F = 1;
                    }
                    else if(b.length() > d.length()){
                        F = 1;
                    }
                }
                else if(op == "<="){
                    if(b.length() == d.length()){//位数相同时，直接字典序比较大小
                        if(d > b) F = 1;
                    }
                    else if(b.length() < d.length()){
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
        relation -> Delete(rocksdb::WriteOptions(),start+"_start");
        relation -> Put(rocksdb::WriteOptions(),start+"_start",new_relation);
        //更新拓扑结构，在pk_start里添加终点

        old_relation = "";
        relation -> Get(rocksdb::ReadOptions(),end+"_end",&old_relation);
        new_relation = old_relation +  pk + ",";
        relation -> Delete(rocksdb::WriteOptions(),end+"_end");
        relation -> Put(rocksdb::WriteOptions(),end+"_end",new_relation);
        //在pk_end添加起点
        //cout << new_relation << endl;
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
        db -> Put(rocksdb::WriteOptions(),pk + "_start",start);
        db -> Put(rocksdb::WriteOptions(),pk + "_end",end);
        propertys += "start,end,type;";
        if(s.ok())  s = db -> Put(rocksdb::WriteOptions(),pk,propertys);
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
        relation -> Delete(rocksdb::WriteOptions(),start+"_start");
        relation -> Put(rocksdb::WriteOptions(),start+"_start",new_relation);
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
        relation -> Delete(rocksdb::WriteOptions(),end+"_end");
        relation -> Put(rocksdb::WriteOptions(),end+"_end",new_relation);

        //cout << new_relation << endl;
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

    void BFS(std::string pk,int num){//从节点pk出发，遍历num层
        std::string propertys = "",value = "";
        bool pk_exist = if_node_exist(pk,propertys,value,db);
        if(!pk_exist) return;
        struct node{
            std::string a;
            int level;
        };
        queue<node> q;
        map<std::string,int> vis;
        node start;
        start.a = pk;
        start.level = 0;
        q.push(start);
        vis[pk] = 1;
        int max_num = 0;
        while(!q.empty()){
            node u = q.front();
            q.pop();
            max_num = max(max_num,u.level);
            if(u.level == num){
                cout << u.a << " ";
                continue;
            }
            std::string edges = "";//存储与当前节点a相连的边
            relation -> Get(rocksdb::ReadOptions(),u.a + "_start",&edges);

            std::string next_pk = "";
            int flag = 0;
            for(int i = 0; i < edges.length(); i++){
                if(flag == 0 && edges[i] == '_') flag++;
                else if(flag == 1 && edges[i] == '_') flag++;
                else if(flag == 2 && edges[i] != ',') next_pk += edges[i];
                else if(flag == 2 && edges[i] == ','){
                    node next;
                    next.a = next_pk;
                    next.level = u.level + 1;
                    if(!vis[next_pk])   q.push(next);
                    next_pk = "";
                    flag = 0;
                }
            }
        }
        if(max_num < num){
            cout << "There is no level " << num << endl;
        }
    }
    void BFS(std::string pk,std::string conditions){//从节点pk出发，找到与pk连通的所有符合查询条件的节点
        std::string propertys = "",value = "";
        bool pk_exist = if_node_exist(pk,propertys,value,db);
        if(!pk_exist) return;

        int cnum = 0;
        std::string condition[50];//用数组存储单独的查询条件
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
        queue<std::string> q;
        q.push(pk);
        map<std::string,int> vis;
        vis[pk] = 1;
        int num = 0;
        while(!q.empty()){
            std::string u = q.front();
            q.pop();
            int F = 0;//表示当前pk符合条件
            for(int i = 0; i <= cnum; i++){
                std::string c = u + "_" + a[i];//要判断的属性
                std::string d = "";//c所对应的属性值
                db -> Get(rocksdb::ReadOptions(),c,&d);
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
                if(num == 1)    cout << "node:  " << u;
                else    cout << "  " << u;
            }

            std::string edges = "";//当前节点u所相连的边edges
            relation -> Get(rocksdb::ReadOptions(),u + "_start",&edges);
            std::string next = "";
            int flag = 0;
            for(int i = 0; i < edges.length(); i++){
                if(flag == 0 && edges[i] == '_') flag++;
                else if(flag == 1 && edges[i] == '_') flag++;
                else if(flag == 2 && edges[i] != ',') next += edges[i];
                else if(flag == 2 && edges[i] == ','){
                    if(!vis[next])   q.push(next);
                    next = "";
                    flag = 0;
                }
            }
        }
        cout << endl;
        if(num > 1)     cout << "there are " << num << " nodes" << endl;
        else    cout << "there is " << num << " node" << endl;
    }
};
void show_relation(Graph g){
    rocksdb::Iterator* it = g.relation->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it -> key().ToString();
        std::string value = it -> value().ToString();
        cout << key << " = " << value << endl;
    }
}
