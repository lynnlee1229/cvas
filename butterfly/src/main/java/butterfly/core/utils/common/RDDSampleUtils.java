package butterfly.core.utils.common;

/**
 * @author Lynn Lee
 * @date 2023/11/14
 **/
public class RDDSampleUtils
{

    /**
     * 根据给定参数计算样本数目的方法。
     *
     * @param numPartitions      分区数目
     * @param totalNumberOfRecords 总记录数目
     * @param givenSampleNumbers  样本数目
     * @return 计算得到的样本数目
     */
    public static int getSampleNumbers(int numPartitions, long totalNumberOfRecords, int givenSampleNumbers) {
        // 如果已给定的样本数目大于零
        if (givenSampleNumbers > 0) {
            // 检查给定的样本数目是否大于总记录数目，若是则抛出异常
            if (givenSampleNumbers > totalNumberOfRecords) {
                throw new IllegalArgumentException("[OGE] 样本数 " + givenSampleNumbers + " 不能大于总记录数 " + totalNumberOfRecords);
            }
            return givenSampleNumbers; // 返回给定的样本数目
        }

        // 确保记录数目 >= 2 * 分区数目
        if (numPartitions > (totalNumberOfRecords + 1) / 2) {
            throw new IllegalArgumentException("[OGE] 分区数 " + numPartitions + " 不能大于总记录数的一半 " + totalNumberOfRecords);
        }

        // 若总记录数目小于 1000，则返回总记录数目
        if (totalNumberOfRecords < 1000) {
            return (int) totalNumberOfRecords;
        }

        // 计算最小样本数目，取 numPartitions * 2L 和总记录数目的最小值
        final long minSampleCnt = Math.min(numPartitions * 2L, totalNumberOfRecords);

        // 返回 minSampleCnt 和 totalNumberOfRecords / 100 的最大值，取整为 int 类型
        return (int) Math.max(minSampleCnt, Math.min(totalNumberOfRecords / 100, Integer.MAX_VALUE));
    }

}
