package com.chen;

import java.io.*;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Metadata implements Serializable {
    private static final long serialVersionUID = 1L;
    private static int numSamples;//数据集数量
    private static List<String> idx_to_name;//数据集索引 到 数据集名称
    private static Map<String,Integer> name_to_idx;//数据集名称 到 数据集索引
    private static Map<Integer, Integer> idx_to_cardinality;//数据集索引 到 数据集基数
    private static Map<Integer,String> idx_to_dataFilePath;//数据集索引 到 数据集文件地址
    private static List<Map.Entry<Integer, Integer>> sortedList;//数据集按基数从大到小排序


    static {
        numSamples = 0;
        idx_to_name = new ArrayList<>();
        name_to_idx = new HashMap<>();
        idx_to_cardinality=new HashMap<>();
        idx_to_dataFilePath=new HashMap<>();
        sortedList=new ArrayList<>();
    }

    public static List<Integer> getAllValues() {//获得sortedList中每个键值对的值，即所有数据集的基数（从大到小排序过的）
        return sortedList.stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    public static List<Map.Entry<Integer, Integer>> getSortedList(){
        return sortedList;
    }

    public static void addcardinality(int datasetindex,int cardinality){
        idx_to_cardinality.put(datasetindex,cardinality);
    }

    public static List<Integer> getCardinalities(List<Integer> datasetIndexs){//输入是数据集索引列表，返回这些数据集对应的基数
        List<Integer> cadinalities=new ArrayList<>();
        for(int index:datasetIndexs){
            cadinalities.add(idx_to_cardinality.get(index));
        }
        return cadinalities;
    }

    public static int getIdxByName(String datasetName){
        return name_to_idx.get(datasetName);
    }

    public static String getDatapathByIdx(int index){
        return idx_to_dataFilePath.get(index);
    }
    //根据数据集索引获得数据集名称
    public static String getNameByIdx(int index){
        if (index>=0 && index<idx_to_name.size()){
            return idx_to_name.get(index);
        }else {
            System.out.println("索引异常");
            return null;
        }
    }
    public static boolean addDataset(String cur_datasetPath) {
        // 数据集文件名
        String datasetName = cur_datasetPath.substring(cur_datasetPath.lastIndexOf("\\") + 1);
        // 检查数据集是否已存在
        if (name_to_idx.containsKey(datasetName)) {
            System.err.println("文件已存在: " + datasetName);
            return false;
        }

        // 将数据集添加到索引和名称映射中
        name_to_idx.put(datasetName, idx_to_name.size());
        idx_to_dataFilePath.put(idx_to_name.size(),cur_datasetPath);
        idx_to_name.add(datasetName);

        // 增加样本数量
        numSamples++;
        return true;
    }

    public static void sortCardinality(){
        // 将键值对集合转换为列表
        List<Map.Entry<Integer, Integer>> list = new ArrayList<>(idx_to_cardinality.entrySet());

        // 使用自定义的比较器对列表进行排序
        Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                // 按值（基数）从大到小排序
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        sortedList=list;

//        // 打印排序后的结果
//        System.out.println("按基数从大到小排序后的结果：");
//        for (Map.Entry<Integer, Integer> entry : sortedList) {
//            System.out.println("索引：" + entry.getKey() + "，基数：" + entry.getValue());
//        }
    }


    public static void serialize(String filePath) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeInt(numSamples);
            oos.writeObject(idx_to_name);
            oos.writeObject(name_to_idx);
            oos.writeObject(idx_to_cardinality);
            oos.writeObject(idx_to_dataFilePath);
//            oos.writeObject(sortedList);
            System.out.println("Metadata 对象已成功序列化到 " + filePath + " 文件中");
        } catch (IOException e) {
            System.err.println("Metadata序列化失败：" + e.getMessage());
        }
    }

    public static void deserialize(String filePath) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            numSamples = ois.readInt();
            idx_to_name = (List<String>) ois.readObject();
            name_to_idx = (Map<String,Integer>) ois.readObject();
            idx_to_cardinality = (HashMap<Integer, Integer>) ois.readObject();
            idx_to_dataFilePath = (Map<Integer,String>) ois.readObject();
//            sortedList = (List<Map.Entry<Integer, Integer>>) ois.readObject();
            System.out.println("Metadata 对象已成功从 " + filePath + " 文件中反序列化");
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Metadata反序列化失败：" + e.getMessage());
        }
    }

}
