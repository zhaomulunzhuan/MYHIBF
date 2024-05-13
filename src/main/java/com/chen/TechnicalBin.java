package com.chen;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

public class TechnicalBin implements Serializable {//一个TB其实就是bloom filter
    private static final long serialVersionUID = 1L;
    private int ibf_index;//所处ibf索引
    private String bin_type;//techincal bin的类型，有split，single，merge
    private List<Integer> datasets;//这个bin存储的数据集列表
    private int child_ibf;//如果这个bin是merge类型，则它下一层有一个ibf
    private int size;//布隆过滤器大小
    private int k;//哈希函数个数
    private BitSet bitarray;

    public TechnicalBin(int m,int hash_num,int ibfIndex,String binType,List<Integer> datasetList) {
        this.size = m;
        this.k = hash_num;
        this.ibf_index=ibfIndex;
        this.bin_type= binType;
        this.bitarray = new BitSet(size);
        this.datasets=datasetList;
        this.child_ibf=-1;
    }

    public void setChild(int child){
        this.child_ibf=child;
    }//merge bin会有孩子节点，孩子节点是一个IBF

    public BitSet getbitarray(){
        return bitarray;
    }

    public void insertElement(String element) {
        List<Long> hashValues = Utils.myHash(element, k, size);
        for (long index : hashValues) {
            int positiveIndex = (int) (index & Long.MAX_VALUE); // 将负数索引转换为正数
            bitarray.set(positiveIndex);
        }
    }

    public boolean test(String kmer) {//检查元素是否存在 按列查询时需要
        List<Long> hashValues = Utils.myHash(kmer, k, size);
        for (long index : hashValues) {
            int positiveIndex = (int) (index & Long.MAX_VALUE); // 将负数索引转换为正数
            if (!bitarray.get(positiveIndex)) {
                return false;
            }
        }
        return true;
    }

    public void batchInsertElements(Set<String> elements) {//批量添加元素
        for (String element : elements) {
            insertElement(element);
        }
    }

    public int getIbf_index() {
        return ibf_index;
    }

    public void setIbf_index(int ibf_index) {
        this.ibf_index = ibf_index;
    }

    public String getBin_type() {
        return bin_type;
    }

    public int getSize(){return size;}

    public int getK(){return k;}

    public void setBin_type(String bin_type) {
        this.bin_type = bin_type;
    }

    public List<Integer> getDatasets() {
        return datasets;
    }

    public void setDatasets(List<Integer> datasets) {
        this.datasets = datasets;
    }

    public int getChild_ibf() {
        return child_ibf;
    }

    public void setChild_ibf(int child_ibf) {
        this.child_ibf = child_ibf;
    }

    public int countOnes() {
        return bitarray.cardinality();
    }

    public void printInfo() {
        System.out.println("IBF Index: " + ibf_index);
        System.out.println("Bin Type: " + bin_type);
        System.out.println("Datasets: " + datasets);
        System.out.println("Child IBF: " + child_ibf);
        System.out.println("Size: " + size);
        System.out.println("K: " + k);
        System.out.println("BitArray中1占比: " + ((double) countOnes() / size) * 100);
    }
}
