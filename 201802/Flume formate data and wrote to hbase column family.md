Flume格式化数据写入hbase column family
===

>微信公众号：苏言论  
理论联系实际，畅言技术与生活。


由于需要从kafka消费格式化数据存储到hbase,格式化的数据类似下面的格式;
```xml
db2\t17.16.10.1\t/var/log/monitor.log\t748992\t2018-02-25T08:11:26.000Z\t18-02-25 16:11:26 ##### sudo su -
```
\\t 为分隔符,随着业务和需求的变化而变化;flume提供AsyncHbaseEventSerializer,AsyncHBaseSink,HbaseEventSerializer等序列化数据方式,但不能满足将数据按格式写入column family的需求,flume提供了接口,重写方法,对event实现自定义处理,需要如下的操作:

1. 准备资源(flume源码);
2. 实现接口;
3. 打包整合到flume并调用新实现的接口;

首先[下载flume 1.8源码](http://www.apache.org/dyn/closer.lua/flume/1.8.0/apache-flume-1.8.0-src.tar.gz),解压后在flume-ng-sinks/flume-ng-hbase-sink分支下新建类(FormatAsyncHbaseEventSerializer),实现AsyncHbaseEventSerializer接口;
```xml
public class FormatAsyncHbaseEventSerializer implements AsyncHbaseEventSerializer {  
  @Override  
  public void initialize(byte[] table, byte[] cf) {  

  }  
  
  @Override  
  public List<PutRequest> getActions() {  
   
  }  
  
  @Override  
  public void cleanUp() {  

  }  
  
	@Override
	public void configure(Context context) {

	}
  
  @Override  
  public void setEvent(Event event) {  

    }  
  
  @Override  
  public void configure(ComponentConfiguration conf) {  

  }  
}  
```
在initialize方法内对写入的表,column family初始化;
```xml
  @Override  
  public void initialize(byte[] table, byte[] cf) {  
    this.table = table;  
    this.cf = cf;  
  }
```
在setEvent方法内对event进行设置,比如根据分隔符分拆event内容,设置hbase row key,row key 这里设置为某一列的内容作为前缀后加其它内容(比如时间序,uuid,timestamp,这些都是可以通过配置定义和改变的);
```xml
  @Override  
  public void setEvent(Event event) {  
    String strBody = new String(event.getBody());  
    //根据分隔符拆分内容
    String[] subBody = strBody.split(this.payloadColumnSplit);  

	    if (subBody.length == this.payloadColumn.length)  
	    {  
	        this.payload = new byte[subBody.length][];  
	        for (int i = 0; i < subBody.length; i++)  
	        {  
	            this.payload[i] = subBody[i].getBytes(Charsets.UTF_8);  
	            //设置hbase row key前缀
	            if ((new String(this.payloadColumn[i]).equals(this.rowSuffixCol)))  
	            {  
	                // rowkey 前缀是某一列的值
	                this.rowSuffix = subBody[i];  
	            }  
	        }  
	    } 
  }
```
在configure方法内定义写出hbase的行为;比如将写入的column family定义在配置文件中,程序运行时动态读取,组成特定的预写入column faimily格式;
```xml
@Override
	public void configure(Context context) {
		//获取配置文件中的columns
		String pCol = context.getString("payloadColumn", "pCol");
		//获取配置文件中的增长column
		String iCol = context.getString("incrementColumn", "iCol");
		//获取配置文件中的row key前缀列
		rowSuffixCol = context.getString("rowPrefixCol", "default");
		//获取配置文件中的row key后缀,默认为系统uuid
		String suffix = context.getString("suffix", "uuid");
		//获取配置文件中的分隔符
		payloadColumnSplit = context.getString("payloadColumnSplit");
		if (pCol != null && !pCol.isEmpty()) {
			if (suffix.equals("timestamp")) {
				keyType = KeyType.TS;
			} else if (suffix.equals("random")) {
				keyType = KeyType.RANDOM;
			} else if (suffix.equals("nano")) {
				keyType = KeyType.TSNANO;
			} else {
				keyType = KeyType.UUID;
			}

			// 从配置文件中读出column。
			String[] pCols = pCol.replace(" ", "").split(",");
			payloadColumn = new byte[pCols.length][];
			for (int i = 0; i < pCols.length; i++) {
				// 列名转为小写
				payloadColumn[i] = pCols[i].toLowerCase().getBytes(Charsets.UTF_8);
			}
		}

		if (iCol != null && !iCol.isEmpty()) {
			incrementColumn = iCol.getBytes(Charsets.UTF_8);
		}
		incrementRow = context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
	}
```
最后通过getActions方法循环按照设定的格式和次序写数到hbase column family;
```xml
  @Override  
  public List<PutRequest> getActions() {  
    List<PutRequest> actions = new ArrayList<PutRequest>();  
    if(payloadColumn != null){  
      byte[] rowKey;  
      try {  
        switch (keyType) {  
          case TS:  
            rowKey = SimpleRowKeyGenerator.getTimestampKey(rowSuffix);  
            break;  
          case TSNANO:  
            rowKey = SimpleRowKeyGenerator.getNanoTimestampKey(rowSuffix);  
            break;  
          case RANDOM:  
            rowKey = SimpleRowKeyGenerator.getRandomKey(rowSuffix);  
            break;  
           default:  
            rowKey = SimpleRowKeyGenerator.getUUIDKey(rowSuffix);  
            break;  
        }  
  
    // for 循环，提交所有列和对于数据的put请求。  
    for (int i = 0; i < this.payload.length; i++)  
    {  
            PutRequest putRequest =  new PutRequest(table, rowKey, cf,payloadColumn[i], payload[i]);  
            actions.add(putRequest);  
    }  
  
      } catch (Exception e){  
        throw new FlumeException("Could not get row key!"+"$payloadColumnSplit=>"+this.payloadColumnSplit+"&payloadColumn.length=>"+this.payloadColumn.length+"&payload=>"+payload, e);  
      }  
    }  
    return actions;  
  }
```
将源码重新打包生成flume-ng-hbase-sink-1.8.0.jar文件,替换FLUME_HOME/lib/下的jar包,修改flume配置;
```xml
flumeConsolidationAgent.sinks.k3.channel= c3
flumeConsolidationAgent.sinks.k3.type = org.apache.flume.sink.hbase.AsyncHBaseSink
flumeConsolidationAgent.sinks.k3.table = syslog
flumeConsolidationAgent.sinks.k3.columnFamily = data
flumeConsolidationAgent.sinks.k3.serializer.payloadColumn=hostname,hostip,logtype,filename,offset,action_time,centent
flumeConsolidationAgent.sinks.k3.serializer = org.apache.flume.sink.hbase.FormatAsyncHbaseEventSerializer
flumeConsolidationAgent.sinks.k3.serializer.payloadColumnSplit=\\\\t
flumeConsolidationAgent.sinks.k3.serializer.rowPrefixCol=hostip
flumeConsolidationAgent.sinks.k3.serializer.suffix=nano
```
type定义sinks 从channel读取数据和写入hbase的类型;   
table 定义写入hbase的表名;  
columnFamily定义表的column family;   
payloadColumn定义column;   
serializer定义处理event的类;   
payloadColumnSplit定义分拆event内容的分隔符;  
rowPrefixCol定义hbase row key前缀;  
suffix定义hbase row key后缀;  

运行程序后检查hbase存储的内容;
```xml
10.1.19.138731500968051947  column=data:action_time, timestamp=1518143825422, value=2018-02-08T08:58:07.000Z 
10.1.19.138731500968051947  column=data:centent, timestamp=1518143825406, value=18-02-08 16:58:07 #####  exit
10.1.19.138731500968051947  column=data:filename, timestamp=1518143825421, value=/var/log/usermonitor/usermonitor.log
10.1.19.138731500968051947  column=data:hostip, timestamp=1518143825402, value=10.1.10.53
10.1.19.138731500968051947  column=data:hostname, timestamp=1518143825402, value=RHTX-HLY-to-jiankong01
10.1.19.138731500968051947  column=data:offset, timestamp=1518143825406, value=107597
```
完整的代码可以在[Flume](https://github.com/LanstonWu/Flume/201802)中获取到;
