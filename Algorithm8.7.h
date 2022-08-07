#include <iostream>
#include <algorithm>
#include <queue>
#include <map>
#include <unordered_map>
#include <stack>
using namespace std;
using namespace rocksdb;
void show_relation(Graph g){//输出所有relation
    rocksdb::Iterator* it = g.relation->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it -> key().ToString();
        std::string value = it -> value().ToString();
        cout << key << " = " << value << endl;
    }
}
void BFS(Graph g,std::string pk,int num){//从节点pk出发，遍历num层
    std::string propertys = "",value = "";
    bool pk_exist = if_node_exist(pk,propertys,value,g.db);
    if(!pk_exist) return;
    struct node{
        std::string a;
        int level;
    };
    queue<node> q;
    unordered_map<std::string,int> vis;
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
        g.relation -> Get(rocksdb::ReadOptions(),u.a + "_start",&edges);

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
void BFS(Graph g,std::string pk,std::string conditions){//从节点pk出发，找到与pk连通的所有符合查询条件的节点
    std::string propertys = "",value = "";
    bool pk_exist = if_node_exist(pk,propertys,value,g.db);
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
    unordered_map<std::string,int> vis;
    vis[pk] = 1;
    int num = 0;
    while(!q.empty()){
        std::string u = q.front();
        q.pop();
        int F = 0;//表示当前pk符合条件
        for(int i = 0; i <= cnum; i++){
            std::string c = u + "_" + a[i];//要判断的属性
            std::string d = "";//c所对应的属性值
            g.db -> Get(rocksdb::ReadOptions(),c,&d);
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
        g.relation -> Get(rocksdb::ReadOptions(),u + "_start",&edges);
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
void BFS_show_path(Graph g,std::string start,std::string end){
    //BFS实现非加权最短路,并输出遍历路径
    std::string propertys = "",value = "";
    bool start_exist = if_node_exist(start,propertys,value,g.db);
    propertys = "",value = "";
    bool end_exist = if_node_exist(end,propertys,value,g.db);
    if(start_exist && end_exist != true) return ;

    queue<std::string> q;
    unordered_map<std::string,int> vis;
    unordered_map<std::string,std::string> pre;
    std::string s = start;
    q.push(s);
    pre[s] = "-1";
    vis[start] = 1;
    int F = 0;
    while(!q.empty()){
        std::string u = q.front();
        vis[u] = 1;
        q.pop();
        if(u == end){
            F = 1;//F=1表示找到了end
            break;
        }
        std::string edges = "";//存储与当前节点a相连的边
        g.relation -> Get(rocksdb::ReadOptions(),u + "_start",&edges);
        std::string next_pk = "";
        int flag = 0;
        for(int i = 0; i < edges.length(); i++){
            if(flag == 0 && edges[i] == '_') flag++;
            else if(flag == 1 && edges[i] == '_') flag++;
            else if(flag == 2 && edges[i] != ',') next_pk += edges[i];
            else if(flag == 2 && edges[i] == ','){
                if(!vis[next_pk]){
                    q.push(next_pk);
                    pre[next_pk] = u;
                }
                next_pk = "";
                flag = 0;
            }
        }
    }
    if(F == 0) cout << start << " can not reach " << end << endl;
    else{
        stack<std::string> st;
        std::string tmp = end;
        while(pre[tmp] != "-1"){
            st.push(tmp);
            tmp = pre[tmp];
        }
        st.push(start);
        while(!st.empty()){
            if(st.top()!=end)
                cout << st.top() << " -> ";
            else cout << st.top() << endl;
            st.pop();
        }
    }
}
unordered_map<std::string,double> dijietesila(Graph g,std::string start){
    //单源最短路,没有负权值
    //优先队列优化版本
    unordered_map<std::string,double> dis;
    unordered_map<std::string,int> vis;
    const double DBL_MAX = numeric_limits<double>::max();
    rocksdb::Iterator* it = g.db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
    int num = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it -> key().ToString();
        std::string value = it -> value().ToString();
        int L = value.length();
        if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
        std::string type;
        g.db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
        if(type != "node") continue;
        dis[key] = DBL_MAX;
    }
    dis[start] = 0;

    struct node
    {
        std::string v;
        double w;
        bool operator < (const node &a) const
        {
            return w > a.w;       //重载<运算符，令w小的优先度高
        }
    };
    priority_queue<node> q;
    node s;
    s.v = start;
    s.w = 0;
    q.push(s);
    while(!q.empty()){
        std::string u = q.top().v;
        q.pop();
        if(vis[u]) continue;
        else vis[u] = 1;

        std::string edges = "";//存储与当前节点a相连的边
        g.relation -> Get(rocksdb::ReadOptions(),u + "_start",&edges);
        std::string next_pk = "";
        std::string edge = "";
        int flag = 0;
        //cout << edges << endl;
        for(int i = 0; i < edges.length(); i++){
            if(edges[i] != ',') edge += edges[i];
            if(flag == 0 && edges[i] == '_'){
                flag++;

            }
            else if(flag == 1 && edges[i] == '_'){
                flag++;

            }
            else if(flag == 2 && edges[i] != ','){
                next_pk += edges[i];

            }
            else if(flag == 2 && edges[i] == ','){
                double w;
                std::string weight = "";
                g.db -> Get(rocksdb::ReadOptions(),edge + "_weight",&weight);
                //cout << edge << endl;
                //cout << weight << endl;
                w = stod(weight);
                std::string t = next_pk;
                if(w + dis[u] < dis[t]){
                    dis[t] = w + dis[u];
                    node next;
                    next.w = dis[t];
                    next.v = t;
                    q.push(next);
                }
                next_pk = "";
                edge = "";
                flag = 0;
            }
        }
    }
    return dis;
}


//服务于并查集
unordered_map<std::string,std::string> f;
//服务于并查集
std::string find_father(std::string pk){
    if(f[pk] != pk) return f[pk] = find_father(f[pk]);
    else return pk;
}
int connected_block_num(Graph g){

    f.clear();
    //初始化  i的father是i自己
    rocksdb::Iterator* it = g.db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it -> key().ToString();
        std::string value = it -> value().ToString();
        int L = value.length();
        if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
        std::string type;
        g.db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
        if(type != "node") continue;
        f[key] = key;
    }
    int num;
    std::string node_num;
    g.db -> Get(rocksdb:: ReadOptions(),"node_num",&node_num);
    num = atoi(node_num.c_str());
    //cout << num << endl;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it -> key().ToString();
        std::string value = it -> value().ToString();
        int L = value.length();
        if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
        std::string type;
        g.db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
        if(type != "edge") continue;
        std::string start= "",end = "";
        g.db -> Get(rocksdb::ReadOptions(),key + "_start",&start);
        g.db -> Get(rocksdb::ReadOptions(),key + "_end",&end);
        std::string fx,fy;
        fx = find_father(start);
        fy = find_father(end);
        //cout << start << " " << end << endl;
        if(fx != fy){
            f[fx] = fy;
            num--;
        }
    }
    return num;
}
const int maxn = 1e6;
struct edge{
    std::string u,v;
    double w;
}a[maxn];
bool cmp(edge a,edge b){
    return a.w < b.w;
}
double Kruscal(Graph g){
    f.clear();
    if(connected_block_num(g) != 1){
        cout << "图g不是连通图" << endl;
        return -1;
    }
    rocksdb::Iterator* it = g.db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it -> key().ToString();
        std::string value = it -> value().ToString();
        int L = value.length();
        if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
        std::string type;
        g.db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
        if(type != "node") continue;
        f[key] = key;
    }
    int num = 0;
    it = g.db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it -> key().ToString();
        std::string value = it -> value().ToString();
        int L = value.length();
        if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
        std::string type;
        g.db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
        if(type != "edge") continue;
        std::string start = "",end = "",weight = "";
        g.db -> Get(rocksdb::ReadOptions(),key + "_start",&start);
        g.db -> Get(rocksdb::ReadOptions(),key + "_end",&end);
        g.db -> Get(rocksdb::ReadOptions(),key + "_weight",&weight);
        num++;
        a[num].u = start;
        a[num].v = end;
        a[num].w = stod(weight);
    }
    sort(a+1,a+1+num,cmp);

    /*for(int i = 1; i <= num; i++)
    {
        cout << a[i].u << " " << a[i].v << " " << a[i].w << endl;
    }*/

    double ans = 0;
    for(int i = 1; i <= num; i++){
        std::string fx = find_father(a[i].u);
        std::string fy = find_father(a[i].v);
        if(fx != fy){
            ans += a[i].w;
            cout << a[i].u << " -> " << a[i].v << endl;
            f[fx] = fy;
        }
    }
    return ans;
}
double Prim(Graph g){
    f.clear();
    if(connected_block_num(g) != 1){
        cout << "图g不是连通图" << endl;
        return -1;
    }
    double ans = 0;
    unordered_map<std::string,double> dis;
    unordered_map<std::string,int> vis;
    const double DBL_MAX = numeric_limits<double>::max();
    std::string start;
    int F = 0;
    rocksdb::Iterator* it = g.db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it -> key().ToString();
        std::string value = it -> value().ToString();
        int L = value.length();
        if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
        std::string type;
        g.db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
        if(type != "node") continue;
        if(F == 0){
            start = key;//找到起点
            F = 1;
        }
        else{
            dis[key] = DBL_MAX;
            //cout << dis[key] << endl;
        }
    }
    vis[start] = 1;
    //cout << start << endl;
    std::string edges = "";
    g.relation -> Get(rocksdb::ReadOptions(),start + "_start",&edges);
    std::string next_pk = "";
    int flag = 0;
    std::string edge = "";
    for(int i = 0; i < edges.length(); i++){
        if(edges[i] != ',') edge += edges[i];
        if(flag == 0 && edges[i] == '_'){
            flag++;
        }
        else if(flag == 1 && edges[i] == '_'){
            flag++;
        }
        else if(flag == 2 && edges[i] != ','){
            next_pk += edges[i];
        }
        else if(flag == 2 && edges[i] == ','){
            double w;
            std::string weight = "";
            g.db -> Get(rocksdb::ReadOptions(),edge + "_weight",&weight);
            w = stod(weight);
            //cout << w << endl;
            if(dis[next_pk] > w)
                dis[next_pk] = w;
            next_pk = "";
            edge = "";
            flag = 0;
        }
    }



    edges = "";
    g.relation -> Get(rocksdb::ReadOptions(),start + "_end",&edges);
    next_pk = "";
    flag = 0;
    edge = "";
    for(int i = 0; i < edges.length(); i++){
        if(edges[i] != ',') edge += edges[i];
        if(flag == 0 && edges[i] == '_'){
            flag++;
        }
        else if(flag == 1 && edges[i] == '_'){
            flag++;
        }
        else if(flag == 1 && edges[i] != '_'){
            next_pk += edges[i];
        }
        else if(flag == 2 && edges[i] == ','){
            double w;
            std::string weight = "";
            g.db -> Get(rocksdb::ReadOptions(),edge + "_weight",&weight);
            //cout << edge << endl;

            w = stod(weight);
            //cout << w << endl;
            //cout << next_pk << " " << w << endl;
            if(dis[next_pk] > w)
                dis[next_pk] = w;
            next_pk = "";
            edge = "";
            flag = 0;
        }
    }


    int count = 1;
    int num;
    std::string node_num;
    g.db -> Get(rocksdb:: ReadOptions(),"node_num",&node_num);
    num = atoi(node_num.c_str());
    //cout << num << endl;
    double min_w;
    std::string t;
    while(count < num){
        min_w = DBL_MAX;

        it = g.db->NewIterator(rocksdb::ReadOptions());//创建一个迭代器
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string key = it -> key().ToString();
            std::string value = it -> value().ToString();
            int L = value.length();
            if(value[L-1] != ';') continue;//判断当前遍历到的key是不是target的pk
            std::string type;
            g.db -> Get(rocksdb::ReadOptions(),key+"_type",&type);
            if(type != "node") continue;
            if(!vis[key] && dis[key] < min_w){
                min_w = dis[key];
                t = key;
            }
        }
        vis[t] = 1;
        //cout << t << " " << min_w << endl;
        count++;
        ans += min_w;



        //问题：如何根据key t来得到边的weight
        //并且，key t之间可能存在多条不同种类的边，怎么确定是哪一种边
        edges = "";
        g.relation -> Get(rocksdb::ReadOptions(),t + "_start",&edges);
        next_pk = "";
        flag = 0;
        std::string edge = "";
        for(int i = 0; i < edges.length(); i++){
            if(edges[i] != ',') edge += edges[i];
            if(flag == 0 && edges[i] == '_'){
                flag++;
            }
            else if(flag == 1 && edges[i] == '_'){
                flag++;
            }
            else if(flag == 2 && edges[i] != ','){
                next_pk += edges[i];
            }
            else if(flag == 2 && edges[i] == ','){
                if(vis[next_pk]) continue;
                double weight_key_t;
                std::string weight = "";
                g.db -> Get(rocksdb::ReadOptions(),edge + "_weight",&weight);
                //cout << edge << endl;
                //cout << weight << endl;
                if(weight != "")
                    weight_key_t = stod(weight);

                if(dis[next_pk] > weight_key_t){
                    dis[next_pk] = weight_key_t;
                    //cout << t << " " <<  next_pk << " " << weight_key_t << endl;
                }
                next_pk = "";
                edge = "";
                flag = 0;
            }
        }


        edges = "";
        g.relation -> Get(rocksdb::ReadOptions(),t + "_end",&edges);
        next_pk = "";
        flag = 0;
        edge = "";
        for(int i = 0; i < edges.length(); i++){
            if(edges[i] != ',') edge += edges[i];
            if(flag == 0 && edges[i] == '_'){
                flag++;
            }
            else if(flag == 1 && edges[i] != '_'){
                next_pk += edges[i];
            }
            else if(flag == 1 && edges[i] == '_'){
                flag++;
            }
            else if(flag == 2 && edges[i] == ','){
                if(vis[next_pk]) continue;
                double weight_key_t;
                std::string weight = "";
                g.db -> Get(rocksdb::ReadOptions(),edge + "_weight",&weight);
                //cout << edge << endl;
                //cout << weight << endl;
                if(weight != "")
                    weight_key_t = stod(weight);

                if(dis[next_pk] > weight_key_t){
                    dis[next_pk] = weight_key_t;
                    //cout << t << " " << next_pk << " " << weight_key_t << endl;
                }
                next_pk = "";
                edge = "";
                flag = 0;
            }
        }
    }
    return ans;
}