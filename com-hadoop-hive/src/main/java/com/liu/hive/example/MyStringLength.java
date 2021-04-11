package com.liu.hive.example;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author LiuJunFeng
 * @date 2021/4/5 下午3:29
 * @description: 自定义 UDF 函数 (UDF（User-Defined-Function 一进一出)，需要继承 GenericUDF 类
 *
 * 打成 jar 包上传到服务器,将 jar 包添加到 hive 的 classpath
 * hive (default)> add jar /opt/module/data/myudf.jar;
 *
 * 创建临时函数与开发好的 java class 关联
 * hive (default)> create temporary function my_len as "com.atguigu.hive.MyStringLength";
 *
 * 即可在 hql 中使用自定义的函数
 * hive (default)> select ename,my_len(ename) ename_len from emp;
 */
public class MyStringLength extends GenericUDF {

	/**
	 * @param arguments 输入参数类型的鉴别器对象
	 * @return 返回值类型的鉴别器对象
	 * @throws UDFArgumentException
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		// 判断输入参数的个数
		if (arguments.length != 1) {
			throw new UDFArgumentLengthException("Input Args Length	Error !!!");
		}
		// 判断输入参数的类型
		if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
		) {
			throw new UDFArgumentTypeException(0, "Input Args Type Error !!!");
		}
		//函数本身返回值为 int，需要返回 int 类型的鉴别器对象
		return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
	}

	/**
	 * 函数的逻辑处理
	 * @param arguments 输入的参数
	 * @return 返回值
	 * @throws HiveException
	 */
	@Override
	public Object evaluate(DeferredObject[] arguments) throws
			HiveException {
		if (arguments[0].get() == null) {
			return 0;
		}
		return arguments[0].get().toString().length();
	}

	@Override
	public String getDisplayString(String[] children) {
		return "";
	}
}


