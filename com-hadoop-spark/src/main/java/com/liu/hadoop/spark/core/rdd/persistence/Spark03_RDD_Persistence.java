package com.liu.hadoop.spark.core.rdd.persistence;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author LiuJunFeng
 * @date 2021/4/13 下午7:45
 * @description: RDD 持久化  cache   persist   checkpoint  区别
 *
 * 缓存和检查点区别
 * 1）Cache 缓存只是将数据保存起来，不切断血缘依赖。Checkpoint 检查点切断血缘依赖。
 * 2）Cache 缓存的数据通常存储在磁盘、内存等地方，可靠性低。Checkpoint 的数据通常存储在 HDFS 等容错、高可用的文件系统，可靠性高。
 * 3）建议对 checkpoint()的 RDD 使用 Cache 缓存，这样 checkpoint 的 job 只需从 Cache 缓存中读取数据即可，否则需要再从头计算一次 RDD。
 */
public class Spark03_RDD_Persistence {

	public static void main(String[] args) {

		/**
		 * @author LiuJunFeng
		 * @date 2021/4/16 上午3:45
		 * @return void
		 * @Description:
		 *
		 * cache: 将数据临时存储在内存中进行数据重用
		 * 		  会在血缘关系中添加新的依赖。一旦，出现问题，可以重头读取数据
		 *
		 * persist : 将数据临时存储在磁盘文件中进行数据重用
		 * 			 涉及到磁盘IO，性能较低，但是数据安全
		 *           如果作业执行完毕，临时保存的数据文件就会丢失
		 *
		 * checkpoint : 将数据长久地保存在磁盘文件中进行数据重用
		 *        		涉及到磁盘IO，性能较低，但是数据安全
		 *       		为了保证数据安全，所以一般情况下，会独立执行作业
		 *        		为了能够提高效率，一般情况下，是需要和cache联合使用
		 *      执行过程中，会切断血缘关系。重新建立新的血缘关系
		 *      checkpoint等同于改变数据源
		 *
		 *
		 */

	}


}
