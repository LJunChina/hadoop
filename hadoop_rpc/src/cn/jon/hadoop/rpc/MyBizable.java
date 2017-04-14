package cn.jon.hadoop.rpc;

public interface MyBizable {
	long versionID = 123456;//该字段必须要有，不然会报java.lang.NoSuchFieldException: versionID异常
	public String doSomething(String str);
}
