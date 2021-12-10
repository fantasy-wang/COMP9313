
/*  Documentation:
 *  For this assignment, firstly, I use hashset to save the nodes which is first row and second row and then calculate the number of nodes.
 * 	Secondly, I use the mapToPair function, for this step, I split the lines, I set first element as key.
 *  For value part, I use Tuple2 and the inside is arraylist and hashmap which is the adjacent node and distance. 
 *  For arraylist , I add the "inf" which represent the distance and add the "unvisited" which represent the node is not been visited. 
 *  For hashmap, I use the second element of split line as key of hashmap and the third element of split line as value of hashmap.
 *  
 *  Thirdly, I set the loop and the condition is node size, I first loop the start node.
 *  Then, inside the loop, I use the flatMapToPair function and reduceByKey function.
 *  
 *  For flatMapToPair step, I get the all values, then, I will remove the duplication element and the order is original order.
 *  Then, I use judge whether they are all "unvisited" for the arraylist. 
 *  If they were and the key is start node, I will change the "inf" to "0" and "unvisited" to "finished" and add them and other original elements to output. 
 *  Then, for hashmap part, if "null" not exit, I loop the hashmap and key is output key and value as distance and then add "visited" and 
 *  the key which is the parent node and {null:0} as hashmap which is the output value.
 *  If there exits "visited" or "finished", then, choose the node which are "visited". 
 *  I change the "visited" to "finished" and other elements not change and store them into output.and store to output. 
 *  Then, for hashmap part, if "null" not exit, I loop the hashmap and key is output key and value as distance and then add "visited" and 
 *  the key which is the parent node and {null:0} as hashmap which is the output value.
 *  For nodes which are "unvisited", store the original to output. Then, return output.iterable.
 *  
 *  For reduce function, I compare the distance and choose the shortest distance and "inf" as a max_value in here. 
 *  And I set the parent nodes and adjacent node and distance of the shortest distance part and output them.
 *  Then, I transfer the output of the reduceByKey to flatMapToPair input to do the loop. and also calculate the number of loop times.
 *  
 *  Forth, I use filter function to remove start node.
 *  Then, I create a class Finalout to store the final output which contains node, distance and path.
 *  Then, I use map function to get node which is key and distance which is first element of arraylist of value (if it is "inf", set it to -1)
 *  and path which is from second element to end of arraylist of value and use "-" to connect and add a key at the end.
 *  
 *  Finally, I use sortBy function to set the order and use the distance as judge evidence. 
 *  If distance is -1 ,then return the max_value, if not, return original distance.
 *  And save file as the third argument of command.
 */ 



import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class AssigTwo {
	
	//define a class as correct format which is (nodes, distance, path)
	public static class Finalout implements Serializable{
		private String nodes;
		private Integer distance;
		private String path;
		
		public Finalout() {
			nodes = "";
			distance = -1;
			path = "";
		}
		
		
		public String getNodes() {
			return nodes;
		}
		public void setNodes(String nodes) {
			this.nodes = nodes;
		}
		public Integer getDistance() {
			return distance;
		}
		public void setDistance(Integer distance) {
			this.distance = distance;
		}
		public String getPath() {
			return path;
		}
		public void setPath(String path) {
			this.path = path;
		}


		@Override
		public String toString() {
			StringBuilder str = new StringBuilder();
			str.append(nodes).append(",").append(distance).append(",").append(path);
			return str.toString();
		}
	}

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("AssigTwo")
				.setMaster("local");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> input = context.textFile(args[1]);
		
//		input.flatMap(s->ArrayList.aslist(s.split(",")))
//		
//		input.FLATmAP(lambda s:s.id).distinct().count();
		
		//calculate the nodes number
		HashSet<String> node = new HashSet<String>();
		input.collect().forEach(a->{
			node.add(a.split(",")[0]);
			node.add(a.split(",")[1]);
		});
		int size = node.size();

		//mapToPair, using this function to split lines and create keys and values
		//keys: start node,  values: distance, end node and distance 
		JavaPairRDD<String, Tuple2<ArrayList<String>, HashMap<String, Integer>>> step1 = 
				input.mapToPair(new PairFunction<String, String, Tuple2<ArrayList<String>, HashMap<String, Integer>>>(){

			@Override
			public Tuple2<String, Tuple2<ArrayList<String>, HashMap<String, Integer>>> call(String line) throws Exception {
				
				String[] lines = line.split(",");
				String start = lines[0];
				String end = lines[1];
				Integer distance = Integer.parseInt(lines[2]);
				
				
				HashMap<String, Integer> hash = new HashMap<String, Integer>();
				hash.put(end, distance);
				
				ArrayList<String> array = new ArrayList<String>();
				array.add("inf");
				array.add("unvisited");
				
				return new Tuple2<String, Tuple2<ArrayList<String>, HashMap<String, Integer>>>(start, new Tuple2<>(array, hash));
			}
		});
		
		
		JavaPairRDD<String, Tuple2<ArrayList<String>, HashMap<String, Integer>>> reduceout = null;
		
		//loop , the time is the nodes number
		int n=0;
		while(n<size) {
			//flatMapToPair, this function visit the nodes and find their adjacent nodes and set neighbor as key and the distance and parent nodes as value 
			//return output iterable
			JavaPairRDD<String,Tuple2<ArrayList<String>, HashMap<String, Integer>>> mapout = 
				step1.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<ArrayList<String>, HashMap<String, Integer>>>>, String,Tuple2<ArrayList<String>, HashMap<String, Integer>>>(){

				@Override
				public Iterator<Tuple2<String, Tuple2<ArrayList<String>, HashMap<String, Integer>>>> call(
						Tuple2<String, Iterable<Tuple2<ArrayList<String>, HashMap<String, Integer>>>> input) throws Exception {
					
					ArrayList<Tuple2<String,Tuple2<ArrayList<String>, HashMap<String, Integer>>>> output = new ArrayList<Tuple2<String,Tuple2<ArrayList<String>, HashMap<String, Integer>>>>();
					ArrayList<String> ar = new ArrayList<String>();
					HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
					ArrayList<Tuple2<ArrayList<String>, HashMap<String, Integer>>> temp = new ArrayList<Tuple2<ArrayList<String>, HashMap<String, Integer>>>();
					ArrayList<String> arr = new ArrayList<String>();
					
					
					//if (input._1.equals("null")==false) {
						//add the values
					input._2.forEach(temp::add);
					for (int i=0; i<temp.size(); i++) {
						temp.get(i)._1.forEach(ar::add);
						temp.get(i)._2.forEach(hashmap::put);
					}	
					
					//remove duplication of arraylist
					arr.add(ar.get(0));
					for (int i=1; i<ar.size(); i++) {
						boolean flag = true;
						for (int j=0; j<arr.size(); j++) {
							if (ar.get(i).equals(arr.get(j))) {
								flag = false;
							}
						}
						if (flag == true){
							arr.add(ar.get(i));
						}
					}
					
					//judge whether all nodes are unvisited
					boolean flag1 = true;
					for (int i=0; i<arr.size(); i++) {
						if (arr.get(i).equals("visited") || arr.get(i).equals("finished")) {
							flag1 = false;
						}
					}

					if (flag1 == true && input._1.equals(args[0])) {
						//deal with the first node
						arr.set(0, "0");
						arr.set(1, "finished");
						Tuple2<ArrayList<String>, HashMap<String, Integer>> va1 = new Tuple2<ArrayList<String>, HashMap<String, Integer>>(arr, hashmap);
						output.add(new Tuple2<>(input._1, va1));
						
						//set with the neighbors 
						for (Entry<String, Integer> entry: hashmap.entrySet()) {
							 String key = entry.getKey();
							 String dis = entry.getValue().toString();
							 
							 if (key.equals("null")==false) {
								 ArrayList<String> s1 = new ArrayList<String>();
								 if (arr.get(0).equals("inf")) {
									 s1.add(dis); 
								 }else {
									 Integer d = Integer.parseInt(arr.get(0))+Integer.parseInt(dis);
									 String di = d.toString();
									 s1.add(di);
								 }
								 s1.add("visited");
								 for (int j=2; j<arr.size(); j++) {
									 s1.add(arr.get(j));
								 }
								 s1.add(input._1);
								 HashMap<String, Integer> hash = new HashMap<String, Integer>();
								 hash.put("null", 0);
								 Tuple2<ArrayList<String>, HashMap<String, Integer>> va2 = new Tuple2<ArrayList<String>, HashMap<String, Integer>>(s1, hash);
								 output.add(new Tuple2<>(key, va2));
							 }								 
						}
					}else {
						//deal with other nodes which have been visited
						if (arr.get(1).equals("visited")){
							arr.set(1, "finished");
							Tuple2<ArrayList<String>, HashMap<String, Integer>> va3 = new Tuple2<ArrayList<String>, HashMap<String, Integer>>(arr, hashmap);
							output.add(new Tuple2<>(input._1, va3));
							//set with the neighbors
							for (Entry<String, Integer> entry: hashmap.entrySet()) {
								 String key = entry.getKey();
								 String dis = entry.getValue().toString();
								 
								 if (key.equals("null")==false) {
									 ArrayList<String> s1 = new ArrayList<String>();
									 if (arr.get(0).equals("inf")) {
										 s1.add(dis); 
									 }else {
										 Integer d = Integer.parseInt(arr.get(0))+Integer.parseInt(dis);
										 String di = d.toString();
										 s1.add(di);
									 }
									 s1.add("visited");
									 for (int j=2; j<arr.size(); j++) {
										 s1.add(arr.get(j));
									 }
									 s1.add(input._1);
									 HashMap<String, Integer> hash = new HashMap<String, Integer>();
									 hash.put("null", 0);
									 Tuple2<ArrayList<String>, HashMap<String, Integer>> va4 = new Tuple2<ArrayList<String>, HashMap<String, Integer>>(s1, hash);
									 output.add(new Tuple2<>(key, va4));
								 }
							}
						}else {
							Tuple2<ArrayList<String>, HashMap<String, Integer>> va5 = new Tuple2<ArrayList<String>, HashMap<String, Integer>>(arr, hashmap);
							output.add(new Tuple2<>(input._1, va5));
						}
					}
					//}
									
					return output.iterator();
				}
			});
			
						
			//reduce step
			//find the short distance 
			reduceout = mapout.reduceByKey(new Function2<Tuple2<ArrayList<String>,HashMap<String,Integer>>
					,Tuple2<ArrayList<String>,HashMap<String,Integer>>
					,Tuple2<ArrayList<String>,HashMap<String,Integer>>>(){
						
						@Override
						public Tuple2<ArrayList<String>, HashMap<String, Integer>> call(
								Tuple2<ArrayList<String>, HashMap<String, Integer>> a,
								Tuple2<ArrayList<String>, HashMap<String, Integer>> b) throws Exception {
							
							ArrayList<String> key = new ArrayList<String>();
							HashMap<String, Integer> value = new HashMap<String, Integer>();
							
							//judge whether there is null in hashmap
							boolean flag = true;
							for (Entry<String, Integer> entry : a._2.entrySet()) {
								if (entry.getKey().equals("null")) {
									flag = false;
								}
							}
							if (flag == false) {
								if (b._1.get(0).equals("inf") || Integer.parseInt(a._1.get(0)) < Integer.parseInt(b._1.get(0))) {
									key.add(a._1.get(0));
									key.add(a._1.get(1));
									for (int i=2; i<a._1.size(); i++) {
										key.add(a._1.get(i));
									}
									value.putAll(b._2);
								}else if (Integer.parseInt(a._1.get(0)) > Integer.parseInt(b._1.get(0))){
									key.addAll(b._1);
									value.putAll(b._2);
								}
							}else {
								if (a._1.get(0).equals("inf") || Integer.parseInt(b._1.get(0)) < Integer.parseInt(a._1.get(0))) {
									key.add(b._1.get(0));
									key.add(b._1.get(1));
									for (int i=2; i<b._1.size(); i++) {
										key.add(b._1.get(i));
									}
									value.putAll(a._2);
								}else if (Integer.parseInt(b._1.get(0)) > Integer.parseInt(a._1.get(0))) {
									key.addAll(a._1);
									value.putAll(a._2);
								}
							}
							
							//return out;
							return new Tuple2<ArrayList<String>, HashMap<String, Integer>>(key, value);
						}	
					});
			n++;
			step1= reduceout;
		}

		//remove the first node and the null
		reduceout.filter(new Function<Tuple2<String, Tuple2<ArrayList<String>, HashMap<String, Integer>>>, Boolean>(){

			@Override
			public Boolean call(Tuple2<String, Tuple2<ArrayList<String>, HashMap<String, Integer>>> input)
					throws Exception {
				
				if(input._1.equals(args[0])) {
					return false;
				}else {
					return true;
				}
			}
		})
		//change the output into correct format 
		.map(new Function<Tuple2<String, Tuple2<ArrayList<String>, HashMap<String, Integer>>>, Finalout>(){

			@Override
			public Finalout call(Tuple2<String, Tuple2<ArrayList<String>, HashMap<String, Integer>>> input)
					throws Exception {
				
				String nodes = input._1;
				Integer distance = -1;
				String str = "";
				String path = new String();
				if (input._2._1.get(0).equals("inf")) {
					distance = -1;
				}else {
					distance = Integer.parseInt(input._2._1.get(0));
					for (int i=2; i<input._2._1.size(); i++) {
						str = str + input._2._1.get(i);
						str = str + "-";
					}
					path = str + input._1.toString();
				}
				
				
				Finalout finalout = new Finalout();
				finalout.setNodes(nodes);
				finalout.setDistance(distance);
				finalout.setPath(path);
				
				return finalout;
			}
			
		})
		//sort by the distance, which should be ascending
		.sortBy(new Function<Finalout, Object>() {

			@Override
			public Object call(Finalout input) throws Exception {
				if (input.getDistance()==-1) {
					return Integer.MAX_VALUE;
				}else {
					return input.getDistance();
				}
			}
			
		}, true, 1)
		//save file
		.saveAsTextFile(args[2]);;
	}
}
	
