import java.awt.Composite;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


@SuppressWarnings("rawtypes")
class CompositeKey implements WritableComparable {

	private String dcode;
	private short month;
	private short wayType;

	public CompositeKey() {
	}

	public CompositeKey(String udid, short datetime, short wayType) {
		this.dcode = udid;
		this.month = datetime;
		this.wayType = wayType;
	}

	public CompositeKey(CompositeKey c){
		this.dcode = c.dcode;
		this.month = c.month;
		this.wayType = c.wayType;		
	}

	@Override
	public String toString() {
		String s = "";
		//		if(wayType == WayType.INBOUND.ordinal())
		//			s="INBOUND: ";
		//		else
		//			s="OUTBOUND: ";

		return (new StringBuilder()).append(s).append(month).append(',').append(dcode).toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		dcode = WritableUtils.readString(in);
		month = (short) WritableUtils.readVInt(in);
		wayType = (short) WritableUtils.readVInt(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, dcode);
		WritableUtils.writeVInt(out, month);
		WritableUtils.writeVInt(out, wayType);
	}

	public String getUDID() {

		return dcode;
	}

	public void setUDID(String udid) {

		this.dcode = udid;
	}

	public short getDatetime() {

		return month;
	}

	public void setDatetime(short datetime) {

		this.month = datetime;
	}

	public int getWayType(){
		return wayType;
	}

	public void setWayType(short wayType){
		this.wayType = wayType;
	}

	@Override
	public int compareTo(Object o) {
		CompositeKey oComp = (CompositeKey) o;
		int result;
		Integer month1 = Integer.valueOf(month);
		Integer month2 = Integer.valueOf(oComp.month);		
		Integer wt1 = Integer.valueOf(wayType);
		Integer wt2	= Integer.valueOf(oComp.wayType);

		result = wt1.compareTo(wt2);
		if(0 == result){
			result = month1.compareTo(month2);
			if (0 == result) {
				result = dcode.compareTo(oComp.dcode);
			}
		}
		return result;
	}
	
	@Override
	public boolean equals(Object o) {
		CompositeKey temp = (CompositeKey) o;

		if (this == temp) 
			return true;
		else if (temp == null || getClass() != temp.getClass()) 
			return false;
		else{
			if(this.dcode == temp.dcode && this.month == temp.month && this.wayType == temp.wayType)
				return true;
			else
				return false;
		}
	}

	@Override
	public int hashCode(){
		String builder = "";
		builder += this.dcode;
		builder += this.month;
		builder += this.wayType;
		return builder.hashCode();
	}

}

public class main {

	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		//LinkedHashMap will keep the keys in the order they are inserted
		//which is currently sorted on natural ordering
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		int sum = 3;
		Map<CompositeKey, Integer> inbound = new LinkedHashMap<CompositeKey, Integer>();		
		Map<CompositeKey, Integer> total = new LinkedHashMap<CompositeKey, Integer>();

		CompositeKey obj = new CompositeKey("aaa", (short)8, (short)1);
		inbound.put(new CompositeKey(obj), sum);


		CompositeKey temp = new CompositeKey(obj.getUDID(), obj.getDatetime(), (short) 0);
		if(total.containsKey(temp))
			System.out.println("chiave uguale: " + temp.toString());
		total.put(new CompositeKey(temp), (total.containsKey(temp)?total.get(temp):0) + sum);

		for (final CompositeKey key : total.keySet()) {
			System.out.println(key.getUDID() + "," + key.getDatetime() + "," + key.getWayType() + " " + total.get(key));
		}

		System.out.println("-----");

		temp = new CompositeKey(obj.getUDID(), obj.getDatetime(), (short) 0);
		if(total.containsKey(temp))
			System.out.println("chiave uguale: " + temp.toString());
		total.put(new CompositeKey(temp), (total.containsKey(temp)?total.get(temp):0) + sum);

		for (final CompositeKey key : total.keySet()) {
			System.out.println(key.getUDID() + "," + key.getDatetime() + "," + key.getWayType() + " " + total.get(key));
		}

		temp = null;
		obj = null;

	}

}