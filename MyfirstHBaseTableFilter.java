package hbase_operations;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class MyfirstHBaseTableFilter {
	public static void main(String[] args) throws IOException {
		HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
		String tableName = "customer";

		try {
			HTablePool pool = new HTablePool(hconfig, 1000);
			HTable table = (HTable) pool.getTable(tableName);                 //table which has to be filtered
			
			// adding the filter with the column name and the value to be used to filter the rows
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("location"), null, CompareOp.EQUAL,  
					Bytes.toBytes("US"));
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			//printing the values that are filtered out
			for (Result r : rs) {
				System.out.println("???rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					System.out.println(
							"??" + new String(keyValue.getFamily()) + "====?:" + new String(keyValue.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
