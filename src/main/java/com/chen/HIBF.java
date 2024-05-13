package com.chen;

import org.apache.lucene.util.LongBitSet;

import java.io.*;
import java.util.*;

public class HIBF implements Serializable {
        private static final long serialVersionUID = 1L;
        private static int min_tb_size;//最小的技术bin的大小（本来想用来控制single bin的大小太小，导致整个HIBF深度太深，目前没用上）
        private static int level_num;//总的层数
        private static Map<Integer,List<Integer>> level_to_ibfindexs;//层级 到 这一层的IBF索引列表
        private static List<IBF> ibfs;//IBF列表，按照ibf全局索引顺序，在树中是宽度优先的顺序
        private static List<LongBitSet> ibfASrows;//将IBF列表中的每个ibf转换为按行查询后的IBF列表

        static {
                min_tb_size=-1;
                level_num=0;
                level_to_ibfindexs=new HashMap<>();
                ibfs=new ArrayList<>();
        }
        public static void add_level(int level_index, int ibf_index, IBF ibf){//层级索引 ibf索引 IBF对象
                if(level_to_ibfindexs.containsKey(level_index)){
                        List<Integer> ibfindexList = level_to_ibfindexs.get(level_index);
                        ibfindexList.add(ibf_index);
                        ibfs.add(ibf);
                }else{
                        List<Integer> ibfindexList = new ArrayList<>();
                        ibfindexList.add(ibf_index);
                        level_to_ibfindexs.put(level_index, ibfindexList);
                        ibfs.add(ibf);
                        level_num++;
                }

        }

        public static void transposeTOrow(){//将IBF列表中的每个ibf转换为按行查询后的IBF列表
                ibfASrows=new ArrayList<>();
                for(IBF ibf:ibfs){
                        LongBitSet ibf_row = ibf.transposeTorow();//转置成按行的
                        ibfASrows.add(ibf_row);
                }
        }

        public static IBF getIbfByIndex(int ibf_index){
                return ibfs.get(ibf_index);
        }//通过IBF的全局索引获取IBF对象

        public static void serialize(String filePath) {
                try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
                        oos.writeInt(min_tb_size);
                        oos.writeInt(level_num);
                        oos.writeObject(level_to_ibfindexs);
                        oos.writeObject(ibfs);
                        // 序列化其他成员变量
                        System.out.println("HIBF 对象已成功序列化到 " + filePath + " 文件中");
                } catch (IOException e) {
                        System.err.println("HIBF序列化失败：" + e.getMessage());
                }
        }

        public static void deserialize(String filePath) {
                try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
                        min_tb_size = ois.readInt();
                        level_num = ois.readInt();
                        level_to_ibfindexs = (Map<Integer, List<Integer>>) ois.readObject();
                        ibfs = (List<IBF>) ois.readObject();
                        System.out.println("HIBF 对象已成功从 " + filePath + " 文件中反序列化");
                } catch (IOException | ClassNotFoundException e) {
                        System.err.println("HIBF反序列化失败：" + e.getMessage());
                }
        }

        public static void printInfo() {
                System.out.println("HIBF Information:");
                System.out.println("Number of Levels: " + level_num);
                for (Map.Entry<Integer, List<Integer>> entry : level_to_ibfindexs.entrySet()) {
                        int level_index = entry.getKey();
                        List<Integer> ibfindexList = entry.getValue();
                        System.out.println("Level " + level_index + "的信息:");
                        for (int ibf_index : ibfindexList) {
                                ibfs.get(ibf_index).printInfo();
                        }
                }
        }


}
