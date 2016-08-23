package gelly;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceEdgesFunction;
import org.apache.flink.graph.ReduceNeighborsFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.graph.library.GSAPageRank;
import org.apache.flink.graph.library.GSATriangleCount;
import org.apache.flink.graph.library.PageRank;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;

public class gelly_routes {

	public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //a. 	Import the logistics data from the CSV files!
        String edgePath = "/home/scads/logistics/datasets/imports/routes.txt";
        String verticesPath = "/home/scads/logistics/datasets/imports/verticesData.txt";

        DataSource<Tuple2<Long, String>> vertices = env.readCsvFile(verticesPath).fieldDelimiter(";").types(Long.class, String.class);
        DataSource<Tuple3<Long, Long, Long>> edges = env.readCsvFile(edgePath).fieldDelimiter(";").types(Long.class, Long.class, Long.class);

		//test with print
        vertices.print();
//        edges.print();

        //b. Create a graph from this Datasets. Each edge represents a connection from one logistic destination to another, and the weight of an edge shows the number of flights (value of the goods) on that connection
        Graph<Long, String, Long> graph = Graph.fromTupleDataSet(vertices, edges, env);

        //c. 	Get familiar with the data.
        //      Count the number of incoming respectively the outgoing connections of a random city/vertexID?

        FilterOperator<Edge<Long, Long>> getOutEdges = graph.getEdges().filter(new FilterFunction<Edge<Long, Long>>() {
            public boolean filter(Edge<Long, Long> longLongEdge) throws Exception {
                
            		//for incoming edges
                    return (longLongEdge.f0 == 34L);
                    
//                    for outgoing edges
                    //return (longLongEdge.f1 == 34L);
            }
        });
        
        long countEdges = getOutEdges.count();
        System.out.println(countEdges);

        // Find out the vertexID of the city "Leipzig"? What is the summed value of all handled goods in Leipzig?
        // What is the summed value of all handled goods in Leipzig? use Gellys Neighborhood methods!
        Graph<Long, String, Long> getVertIdLeipzig = graph.filterOnVertices(new FilterFunction<Vertex<Long, String>>() {
            public boolean filter(Vertex<Long, String> vertex) throws Exception {

                return (vertex.f1.equals("Leipzig"));
            }
        });
        getVertIdLeipzig.getVertexIds().print();
        
        //What is the overall value of goods in Leipzig? Compute the summed value for incoming and outgoing connections.
        AggregateOperator<Edge<Long, Long>> sumValue = graph.filterOnEdges(new FilterFunction<Edge<Long, Long>>() {
	            public boolean filter(Edge<Long, Long> longLongEdge) throws Exception {
	                if (longLongEdge.f0 == 196L || longLongEdge.f1 == 196L)
	                    return true;
	                else
	                    return false;
	            }
	        })
	        .getEdges()
	        .sum(2);
        sumValue.print();


      //d. graph transformations
        //add a vertex with the ID 2177L and your Name!
        //add some edges to locations of your choice
        Graph<Long, String, Long> addedGraph = graph
            .addVertex(new Vertex<Long, String>(2177L, "NormanSp"))
            .addEdge(new Vertex<Long, String>(2177L, "NormanSp"), new Vertex<Long, String>(196L, "Leipzig"), 1606L)
            .addEdge(new Vertex<Long, String>(2177L, "NormanSp"), new Vertex<Long, String>(137L, "Berlin"), 1988L);
                  
        //extract a subgraph, containing your added vertice, the cities Leipzig and 4 locations of your choice
        //exclude connections with transported goods value less then 1000 €. Sort them...
        Graph<Long, String, Long> sub = addedGraph.subgraph(
                new FilterFunction<Vertex<Long, String>>() {
                    public boolean filter(Vertex<Long, String> longStringVertex) throws Exception {
                    	
                    	return (longStringVertex.f0 == 196L || longStringVertex.f0 == 137L
                                || longStringVertex.f0 == 2177L || longStringVertex.f0 == 417L
                                || longStringVertex.f0 == 651L || longStringVertex.f0 == 133L);
                    }
                },
                new FilterFunction<Edge<Long, Long>>() {
                    public boolean filter(Edge<Long, Long> longLongEdge) throws Exception {
                        if (longLongEdge.f2 > 1000)
                            return true;
                        else
                            return false;
                    }
                });

        sub.getEdges().print();
        sub.getVertices().print();


       		//Sort them in descending order.
        // what is the problem with sorting in distributed systems???
        SortPartitionOperator maxValues = graph.reduceOnEdges(new ReduceEdgesFunction() {
        	
            	public Long reduceEdges(Object o1, Object o2) {
            		return Math.max(Long.parseLong(o1.toString()), Long.parseLong(o2.toString()));
            	}
        	}, EdgeDirection.IN)
           .partitionByRange(1)
           .sortPartition(1, Order.DESCENDING);
//        maxValues.print();
        
      //f.    Identify Connected Components in the graph
      //      Explain the results. Why is there just on component?
      //      Adjust the code to compute only components with value less then 10
        Graph<Long, Long, Long> conCom = Graph.fromTupleDataSet(edges, new MapFunction<Long, Long>() {

            public Long map(Long aLong) throws Exception {
                return aLong;
            }
        }, env)
                .filterOnEdges(new FilterFunction<Edge<Long, Long>>() {
                    public boolean filter(Edge<Long, Long> longLongEdge) throws Exception {
                        return longLongEdge.f2 < 10;
                    }
                });
        DataSet<Vertex<Long, Long>> result = conCom.run(new GSAConnectedComponents<Long, Long>(50));
        result.print();

        //g.    Compute a Pagerank on the given edges. During graph creation, implement a map function to give each vertex a initial value of 1.0
       DataSet<Edge<Long, Double>> pagerankData =  env.readCsvFile(edgePath).fieldDelimiter(";").types(Long.class, Long.class, Double.class)
    		   										  .map(new Tuple3ToEdgeMap<Long, Double>());
       
       Graph<Long, Double, Double> PRgraph = Graph.fromDataSet(pagerankData, new MapFunction<Long, Double>() {
			public Double map(Long arg0) throws Exception {
				// TODO Auto-generated method stub
				return 1.0d;
			}}, env);
             
       DataSet<Vertex<Long, Double>> PageRank = PRgraph.run(new PageRank<Long>(0.85, 25));
//       PageRank.print();       

        //env.execute("Gelly logistics example");
    }
}
