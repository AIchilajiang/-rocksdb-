#include <iostream>
#include <algorithm>
#include "testDB.h"
using namespace std;
using namespace rocksdb;
int main(){

    Graph g = Graph();

    //g.add_node("wzy","name=wzy,age=88,address=AAAA");
    //g.add_edge("lose_wzy_game","date=22.4.14");
    //g.add_node("wsc","name=wsc,age=21,address=china");
    //g.add_node("game","date=2.15");
    //g.add_edge("win_wsc_game","date=22.4.14");

    //g.update_node("wsc","name=wzy,age=55,address=MOON");
    //g.update_edge("win_wsc_game","date=999,time=22.12");

    //g.delete_node("wsc");
    //g.delete_edge("win_wsc_game");

    //g.select_node("age>=88,name=wzy");
    //g.show_node("wsc");
    //g.show_edge("win_wsc_game");
    //g.show_node("wzy");
    //g.show_edge("lose_wzy_game");

    //g.all_node();
    //g.all_edge();

    //show_relation(g);



    //g.clear_all();
    g.closeGraph();
}