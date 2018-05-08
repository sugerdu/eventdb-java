package org.osv.eventdb.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class MDRegionObserver extends BaseRegionObserver {
	/**
	 * Called before the client performs a Get.
	 * <p>
	 * customer commands: get 'tableName', 'bucketRowkey', 'data:__MDQUERY__',
	 * ['data:__INDEXONLY__',] 'data:p#property#start#startKey',
	 * 'data:p#property#end#endKey', 'data:p#property#get#rowkey'
	 * <p>
	 * Call CoprocessorEnvironment#bypass to skip default actions
	 * <p>
	 * Call CoprocessorEnvironment#complete to skip any subsequent chained
	 * coprocessors
	 * 
	 * @param c      the environment provided by the region server
	 * @param get    the Get request
	 * @param result The result to return to the client if default processing is
	 *               bypassed. Can be modified. Will not be used if default
	 *               processing is not bypassed.
	 * @throws IOException if an error occurred on the coprocessor
	 */
	@Override
	public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get, final List<Cell> result)
			throws IOException {
		Map<byte[], NavigableSet<byte[]>> colMap = get.getFamilyMap();
		if (colMap == null)
			return;
		NavigableSet<byte[]> cols = colMap.get(Bytes.toBytes("data"));
		if (cols == null)
			return;
		Set<String> colsSet = new HashSet<String>();
		for (byte[] col : cols)
			colsSet.add(Bytes.toString(col));
		// common get
		if (!colsSet.contains("__MDQUERY__"))
			return;
		// return bit-index or event result
		boolean indexOnly = colsSet.contains("__INDEXONLY__");

		// get single row
		Map<String, List<Integer>> getOp = new HashMap<String, List<Integer>>();
		// scan multiple rows
		Map<String, List<Integer>> scanOp = new HashMap<String, List<Integer>>();
		Map<String, List<Integer>> op = null;
		// fomat query commands of perperties
		for (String propStr : colsSet) {
			if (propStr.startsWith("p#")) {
				String[] splits = propStr.split("#");
				if (splits.length != 4)
					throw new IOException("__MDQUERY__ PARAMETER(" + propStr + ") ERROR!");
				if ("get".equals(splits[2]))
					op = getOp;
				else
					op = scanOp;
				List<Integer> paramList = op.get(splits[1]);
				if (paramList == null) {
					paramList = new ArrayList<Integer>();
					paramList.add(Integer.valueOf(splits[3]));
					op.put(splits[1], paramList);
				} else {
					paramList.add(Integer.valueOf(splits[3]));
				}
			}
		}

		// query commands
		for (Map.Entry<String, List<Integer>> entry : getOp.entrySet()) {
			List<Integer> paramList = entry.getValue();
			Collections.sort(paramList);
		}
		for (Map.Entry<String, List<Integer>> entry : scanOp.entrySet()) {
			List<Integer> paramList = entry.getValue();
			if (paramList.size() != 2)
				throw new IOException("__MDQUERY__ PARAMETER(" + entry.getKey() + ") ERROR!");
			Collections.sort(paramList);
		}

		// region operation
		Region hregion = c.getEnvironment().getRegion();

		// test scan commands
		byte[] scanResult = Bytes.toBytes("");
		for (Map.Entry<String, List<Integer>> entry : scanOp.entrySet()) {
			List<Integer> paramList = entry.getValue();
			byte[] start = Bytes.add(get.getRow(), Bytes.toBytes("#" + entry.getKey() + "#"),
					Bytes.toBytes(String.format("%03d", paramList.get(0))));
			byte[] end = Bytes.add(get.getRow(), Bytes.toBytes("#" + entry.getKey() + "#"),
					Bytes.toBytes(String.format("%03d", paramList.get(1))));
			Filter filter = new InclusiveStopFilter(end);
			Scan scan = new Scan();
			scan.setStartRow(start);
			scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"));
			scan.setFilter(filter);
			RegionScanner regionScanner = hregion.getScanner(scan);
			List<Cell> internalResult = null;
			boolean hasnext = true;
			do {
				internalResult = new ArrayList<Cell>();
				hasnext = regionScanner.next(internalResult);
				if (internalResult != null && internalResult.size() > 0) {
					byte[] singleRowValue = CellUtil.cloneValue(internalResult.get(0));
					scanResult = Bytes.add(scanResult, singleRowValue);
				}
			} while (hasnext);
			regionScanner.close();
		}

		result.add(new KeyValue(get.getRow(), Bytes.toBytes("data"), Bytes.toBytes("value"), scanResult));

		c.bypass();
	}

	/**
	 * Called before the client stores a value.
	 * <p>
	 * customer commands: put 'tableName', 'rowkey', 'data:__MDINSERT__' => 'append'
	 * | 'prepend', 'data:value' => 'value'
	 * <p>
	 * Call CoprocessorEnvironment#bypass to skip default actions
	 * <p>
	 * Call CoprocessorEnvironment#complete to skip any subsequent chained
	 * coprocessors
	 * 
	 * @param c          the environment provided by the region server
	 * @param put        The Put object
	 * @param edit       The WALEdit object that will be written to the wal
	 * @param durability Persistence guarantee for this Put
	 * @throws IOException if an error occurred on the coprocessor
	 */
	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, final Put put, final WALEdit edit,
			final Durability durability) throws IOException {
		// command put
		List<Cell> mdInsert = put.get(Bytes.toBytes("data"), Bytes.toBytes("__MDINSERT__"));
		if (mdInsert == null || mdInsert.size() == 0)
			return;
		String mdOp = Bytes.toString(CellUtil.cloneValue(mdInsert.get(0)));
		// append or prepend
		if ("append".equals(mdOp) || "prepend".equals(mdOp)) {
			List<Cell> valueCells = put.get(Bytes.toBytes("data"), Bytes.toBytes("value"));
			if (valueCells == null || valueCells.size() == 0)
				return;
			byte[] value = CellUtil.cloneValue(valueCells.get(0));
			byte[] rowkey = put.getRow();
			Region hregion = c.getEnvironment().getRegion();
			Result oldValueResult = hregion.get(new Get(rowkey));
			Cell oldValueCell = oldValueResult.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("value"));
			byte[] oldValue = CellUtil.cloneValue(oldValueCell);
			byte[] newValue;
			if ("append".equals(mdOp))
				newValue = Bytes.add(oldValue, value);
			else
				newValue = Bytes.add(value, oldValue);
			Put newPut = new Put(rowkey);
			newPut.addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), newValue);
			hregion.put(newPut);
		}
		c.bypass();
	}
}
