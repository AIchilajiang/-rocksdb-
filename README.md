# -rocksdb-
ubuntu 20.04.4  rocksdb  c++


需要将rocksdb编译后的文件夹复制到main根目录下（也可在CMkaeLists.txt中修改自己的路径，不同人的路径不同）

目前代码讲解：
    仅使用一个rocksdb::DB*  db来对node/edge的属性进行存储，下文当中将node/edge统称为   target。
    默认认为，可以找到一个主键PK来唯一地标识traget。
    我们可以使用主键PK来找到这个target的各项属性以及属性值。
    
    例如：  
    有一个target：     pk = "姚明"
    对于target 姚明有哪些属性，我们需要对其进行存储：     身高，age，school，birthday.....
    那么我们只需要将 pk -> 属性集 以kv的方式进行存储即可
              即："姚明" -> "身高,age,school,birthday"
              需要注意的是，为了编码规范，我们一律使用英文逗号来对不同属性进行划分，便于字符串遍历时取出不同属性名
              
    对于具体属性的属性值，我们也以kv方式进行存储
              "姚明_身高" ->  "2.21m"
              "姚明_age" ->  "40"
              "姚明_school" ->  "BUPT"
              "姚明_birthday" ->  "6.2"
    
    
    
    
    
    
    
    以下是对  add_node  add_edge的代码讲解
    首先是对于输入的规范要求：  
              pk   属性
              "姚明"  "age=40,birthday=6.2,school=BUPT"
    便于通过遍历输入的字符串，使用 = 和 ，来对单词进行划分
    在添加完各项属性之后，还需要手动添加一个type属性，来对target进行区分，是node还是edge
    
    ps：以后还需要加入一些判断：    在添加target时判断是否已经存在pk相同的target？
                                  node和edge的pk是否可以相同？
    
