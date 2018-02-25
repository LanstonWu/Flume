package org.apache.flume.sink.hbase;  
  
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;  
import java.util.List;
import java.util.Random;
import org.apache.flume.Context;  
import org.apache.flume.Event;  
import org.apache.flume.FlumeException;  
import org.hbase.async.AtomicIncrementRequest;  
import org.hbase.async.PutRequest;  
import org.apache.flume.conf.ComponentConfiguration;  
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;  
import com.google.common.base.Charsets ;  
  
public class FormatAsyncHbaseEventSerializer implements AsyncHbaseEventSerializer {  
  private byte[] table;  
  private byte[] cf;  
  private byte[][] payload;  
  private byte[][] payloadColumn;  
  private  String payloadColumnSplit;  
  private byte[] incrementColumn;  
  private String rowSuffix;  
  private String rowSuffixCol;  
  private byte[] incrementRow;  
  private KeyType keyType;  
  
  @Override  
  public void initialize(byte[] table, byte[] cf) {  
    this.table = table;  
    this.cf = cf;  
  }  
  
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
  
  public List<AtomicIncrementRequest> getIncrements(){  
    List<AtomicIncrementRequest> actions = new  
        ArrayList<AtomicIncrementRequest>();  
    if(incrementColumn != null) {  
      AtomicIncrementRequest inc = new AtomicIncrementRequest(table,  
          incrementRow, cf, incrementColumn);  
      actions.add(inc);  
    }  
    return actions;  
  }  
  
  @Override  
  public void cleanUp() {  
    // TODO Auto-generated method stub  
  }  
  
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
  
  @Override  
  public void setEvent(Event event) {  
    String strBody = new String(event.getBody());  
    //根据分隔符拆分内容
    String[] subBody = strBody.split(this.payloadColumnSplit);  

    /* 记录日志     
    String jsonStr="strBody=>"+strBody+"$payloadColumnSplit=>"+this.payloadColumnSplit+"&subBody.length=>"+subBody.length+"&payloadColumn.length=>"+this.payloadColumn.length;
    BufferedWriter bw=null;
    try{
        File file = new File("/tmp/hanatest.log");
        String content;
   
        if (!file.exists()) {
            	file.createNewFile();
        } 
   
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
         bw = new BufferedWriter(fw);          
        bw.write(jsonStr);       
    }catch (Exception e) {
		e.printStackTrace();
	}finally{
		try{
		 bw.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}*/
    
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
  
  @Override  
  public void configure(ComponentConfiguration conf) {  
    // TODO Auto-generated method stub  
  }  
}  