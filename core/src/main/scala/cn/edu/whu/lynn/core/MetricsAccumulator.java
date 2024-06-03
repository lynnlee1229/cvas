package cn.edu.whu.lynn.core;

import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Lynn Lee
 * @date 2024/3/7
 **/
public class MetricsAccumulator implements Serializable {
    private LongAccumulator leftCount;
    private LongAccumulator rightCount;
    private LongAccumulator mbrCount;
    private LongAccumulator resultCount;
    private DoubleAccumulator joinPreparationTime;
    private DoubleAccumulator filterTime;
    private DoubleAccumulator quadSplitTime;
    private DoubleAccumulator joinTime;
    private DoubleAccumulator algorithmTime;
    private DoubleAccumulator fullProcessTime;
    private DoubleAccumulator fullProcessTimeAll;
    private DoubleAccumulator throughput;
    private DoubleAccumulator throughputAll;
    private Map<String, Object> params;

    public MetricsAccumulator() {

    }

    public MetricsAccumulator setFullProcessTimeAll(DoubleAccumulator fullProcessTimeAll) {
        this.fullProcessTimeAll = fullProcessTimeAll;
        return this;
    }

    public MetricsAccumulator setThroughputAll(DoubleAccumulator throughputAll) {
        this.throughputAll = throughputAll;
        return this;
    }

    public MetricsAccumulator setAlgorithmTime(DoubleAccumulator algorithmTime) {
        this.algorithmTime = algorithmTime;
        return this;
    }

    public MetricsAccumulator setParams(Map params) {
        this.params = params;
        return this;
    }

    public MetricsAccumulator setThroughput(DoubleAccumulator throughput) {
        this.throughput = throughput;
        return this;
    }

    public MetricsAccumulator setFullProcessTime(DoubleAccumulator fullProcessTime) {
        this.fullProcessTime = fullProcessTime;
        return this;
    }

    public MetricsAccumulator setFilterTime(DoubleAccumulator filterTime) {
        this.filterTime = filterTime;
        return this;
    }

    public MetricsAccumulator setJoinPreparationTime(DoubleAccumulator joinPreparationTime) {
        this.joinPreparationTime = joinPreparationTime;
        return this;
    }

    public MetricsAccumulator setLeftCount(LongAccumulator leftCount) {
        this.leftCount = leftCount;
        return this;
    }

    public MetricsAccumulator setRightCount(LongAccumulator rightCount) {
        this.rightCount = rightCount;
        return this;
    }

    public MetricsAccumulator setMbrCount(LongAccumulator mbrCount) {
        this.mbrCount = mbrCount;
        return this;
    }

    public MetricsAccumulator setResultCount(LongAccumulator resultCount) {
        this.resultCount = resultCount;
        return this;
    }

    public MetricsAccumulator setQuadSplitTime(DoubleAccumulator quadSplitTime) {
        this.quadSplitTime = quadSplitTime;
        return this;
    }

    public MetricsAccumulator setJoinTime(DoubleAccumulator joinTime) {
        this.joinTime = joinTime;
        return this;
    }

    public LongAccumulator getLeftCount() {
        return leftCount;
    }

    public LongAccumulator getRightCount() {
        return rightCount;
    }

    public LongAccumulator getMbrCount() {
        return mbrCount;
    }

    public LongAccumulator getResultCount() {
        return resultCount;
    }

    public DoubleAccumulator getQuadSplitTime() {
        return quadSplitTime;
    }

    public DoubleAccumulator getJoinTime() {
        return joinTime;
    }

    public DoubleAccumulator getJoinPreparationTime() {
        return joinPreparationTime;
    }

    public DoubleAccumulator getAlgorithmTime() {
        return algorithmTime;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public DoubleAccumulator getFullProcessTimeAll() {
        return fullProcessTimeAll;
    }

    public DoubleAccumulator getThroughputAll() {
        return throughputAll;
    }

    public void calcJoinPreparationTime() {

        Double fullProcessTime = 0.0, jointime = 0.0, quadSplitTime = 0.0, algorithmTime = 0.0;
        if (this.fullProcessTime != null) {
            fullProcessTime = this.fullProcessTime.value();
        }
        if (this.joinTime != null) {
            jointime = this.joinTime.value();
        }
        if (this.quadSplitTime != null) {
            quadSplitTime = this.quadSplitTime.value();
        }
        if (this.algorithmTime != null) {
            algorithmTime = this.algorithmTime.value();
        }
        joinPreparationTime.add(fullProcessTime - jointime - quadSplitTime - algorithmTime);
    }

    public DoubleAccumulator getFilterTime() {
        return filterTime;
    }

    public DoubleAccumulator getFullProcessTime() {
        return fullProcessTime;
    }

    public DoubleAccumulator getThroughput() {
        return throughput;
    }

    public String toJSON() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (leftCount != null) {
            sb.append("\"leftCount\":").append(leftCount.value()).append(",");
        }
        if (rightCount != null) {
            sb.append("\"rightCount\":").append(rightCount.value()).append(",");
        }
        if (mbrCount != null) {
            sb.append("\"mbrCount\":").append(mbrCount.value()).append(",");
        }
        if (resultCount != null) {
            sb.append("\"resultCount\":").append(resultCount.value()).append(",");
        }
        if (joinPreparationTime != null) {
            sb.append("\"joinPreparationTime\":").append(joinPreparationTime.value()).append(",");
        }
        if (filterTime != null) {
            sb.append("\"filterTime\":").append(filterTime.value()).append(",");
        }
        if (quadSplitTime != null) {
            sb.append("\"quadSplitTime\":").append(quadSplitTime.value()).append(",");
        }
        if (joinTime != null) {
            sb.append("\"joinTime\":").append(joinTime.value()).append(",");
        }
        if (algorithmTime != null) {
            sb.append("\"algorithmTime\":").append(algorithmTime.value()).append(",");
        }
        if (fullProcessTime != null) {
            sb.append("\"fullProcessTime\":").append(fullProcessTime.value()).append(",");
        }
        if (throughput != null) {
            sb.append("\"throughput\":").append(throughput.value()).append(",");
        }
        if (fullProcessTimeAll != null) {
            sb.append("\"fullProcessTimeAll\":").append(fullProcessTimeAll.value()).append(",");
        }
        if (throughputAll != null) {
            sb.append("\"throughputAll\":").append(throughputAll.value()).append(",");
        }
        if (params != null) {
            sb.append("\"params\":{");
            for (String key : params.keySet()) {
                sb.append("\"").append(key).append("\":").append(params.get(key)).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("}");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("}");
        return sb.toString();
    }

    public String toCSV(Boolean withHeader) {
        StringBuilder sb = new StringBuilder();

        // 如果withHeader为true，则调用生成header的方法
        if (withHeader) {
            sb.append(generateHeader()).append("\n");
        }

        // 拼接内容
        if (leftCount != null) {
            sb.append(leftCount.value()).append(",");
        }
        if (rightCount != null) {
            sb.append(rightCount.value()).append(",");
        }
        if (mbrCount != null) {
            sb.append(mbrCount.value()).append(",");
        }
        if (resultCount != null) {
            sb.append(resultCount.value()).append(",");
        }
        if (joinPreparationTime != null) {
            sb.append(joinPreparationTime.value()).append(",");
        }
        if (filterTime != null) {
            sb.append(filterTime.value()).append(",");
        }
        if (quadSplitTime != null) {
            sb.append(quadSplitTime.value()).append(",");
        }
        if (joinTime != null) {
            sb.append(joinTime.value()).append(",");
        }
        if (algorithmTime != null) {
            sb.append(algorithmTime.value()).append(",");
        }
        if (fullProcessTime != null) {
            sb.append(fullProcessTime.value()).append(",");
        }
        if (throughput != null) {
            sb.append(throughput.value()).append(",");
        }
        if (fullProcessTimeAll != null) {
            sb.append(fullProcessTimeAll.value()).append(",");
        }
        if (throughputAll != null) {
            sb.append(throughputAll.value()).append(",");
        }
        if (params != null) {
            for (String key : params.keySet()) {
                sb.append(params.get(key)).append(",");
            }
        }
        // 删除末尾的逗号
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }

        return sb.toString();
    }

    // 生成header的私有方法
    private String generateHeader() {
        StringBuilder header = new StringBuilder();
        if (leftCount != null) {
            header.append("leftCount,");
        }
        if (rightCount != null) {
            header.append("rightCount,");
        }
        if (mbrCount != null) {
            header.append("mbrCount,");
        }
        if (resultCount != null) {
            header.append("resultCount,");
        }
        if (joinPreparationTime != null) {
            header.append("joinPreparationTime,");
        }
        if (filterTime != null) {
            header.append("filterTime,");
        }
        if (quadSplitTime != null) {
            header.append("quadSplitTime,");
        }
        if (joinTime != null) {
            header.append("joinTime,");
        }
        if (algorithmTime != null) {
            header.append("algorithmTime,");
        }
        if (fullProcessTime != null) {
            header.append("fullProcessTime,");
        }
        if (throughput != null) {
            header.append("throughput,");
        }
        if (fullProcessTimeAll != null) {
            header.append("fullProcessTimeAll,");
        }
        if (throughputAll != null) {
            header.append("throughputAll,");
        }
        if (params != null) {
            for (String key : params.keySet()) {
                header.append(key).append(",");
            }
        }
        // 删除末尾的逗号
        if (header.length() > 0) {
            header.deleteCharAt(header.length() - 1);
        }
        return header.toString();
    }


    public void printMetrics() {
        if (leftCount != null) {
            System.out.println("Left RDD count: " + leftCount.value());
        }
        if (rightCount != null) {
            System.out.println("Right RDD count: " + rightCount.value());
        }
        if (mbrCount != null) {
            System.out.println("MBR count: " + mbrCount.value());
        }
        if (resultCount != null) {
            System.out.println("Result count: " + resultCount.value());
        }
        if (joinPreparationTime != null) {
            System.out.println("Join preparation time: " + joinPreparationTime.value());
        }
        if (filterTime != null) {
            System.out.println("Filter time: " + filterTime.value());
        }
        if (quadSplitTime != null) {
            System.out.println("Quad split time: " + quadSplitTime.value());
        }
        if (joinTime != null) {
            System.out.println("Join time: " + joinTime.value());
        }
        if (algorithmTime != null) {
            System.out.println("Algorithm time: " + algorithmTime.value());
        }
        if (fullProcessTime != null) {
            System.out.println("Full process time: " + fullProcessTime.value());
        }
        if (throughput != null) {
            System.out.println("Throughput: " + throughput.value());
        }
        if (fullProcessTimeAll != null) {
            System.out.println("Full process time all: " + fullProcessTimeAll.value());
        }
        if (throughputAll != null) {
            System.out.println("Throughput all: " + throughputAll.value());
        }
        for (String key : params.keySet()) {
            System.out.println(key + ": " + params.get(key));
        }
    }
}

