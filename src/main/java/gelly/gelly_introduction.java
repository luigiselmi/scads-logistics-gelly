package gelly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.library.GSAPageRank;
import org.apache.flink.graph.library.PageRank;
import org.apache.flink.graph.validation.InvalidVertexIdsValidator;
import org.apache.flink.types.NullValue;

import java.util.LinkedList;
import java.util.List;

public class gelly_introduction {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create 4 vertices and 7 edges manually. 
        // The vertice should contain values for an identifier, a city name. 
        // The edges should contain both IDs and a roughly distance between both cities.

        Vertex<Long, String> A = new Vertex<Long, String>(1L, "Hamburg");
        Vertex<Long, String> B = new Vertex<Long, String>(2L, "Berlin");
        Vertex<Long, String> C = new Vertex<Long, String>(3L, "Munich");
        Vertex<Long, String> D = new Vertex<Long, String>(4L, "Frankfurt");

            //edges and distance between the cities
        Edge<Long, Double> e1 = new Edge<Long, Double>(1L, 2L, 290.0);
        Edge<Long, Double> e2 = new Edge<Long, Double>(1L, 3L, 850.0);
        Edge<Long, Double> e3 = new Edge<Long, Double>(2L, 1L, 290.0);
        Edge<Long, Double> e4 = new Edge<Long, Double>(4L, 1L, 350.0);
        Edge<Long, Double> e5 = new Edge<Long, Double>(3L, 4L, 500.0);
        Edge<Long, Double> e6 = new Edge<Long, Double>(2L, 3L, 680.0);
        Edge<Long, Double> e7 = new Edge<Long, Double>(4L, 2L, 500.0);
//        Edge<Long, Double> e6 = new Edge<Long, Double>(5L, 1L, 120.0);

        // 1b
        e1.getValue();
        System.out.println(e1.getValue());
        System.out.println(A.getValue());

        // create vertice dataset
        DataSet<Vertex<Long, String>> vertices = env.fromElements(A, B, C, D);

        //create a list of edges
        List<Edge<Long, Double>> edgeList = new LinkedList<Edge<Long, Double>>();
        edgeList.add(e1);
        edgeList.add(e2);
        edgeList.add(e3);
        edgeList.add(e4);
        edgeList.add(e5);
        edgeList.add(e6);
        edgeList.add(e7);
        
        //create edge dataset from the list
        DataSet<Edge<Long, Double>> edges = env.fromCollection(edgeList);
        //create a graph from edges and vertices
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);
        
        //validate your graph!
        System.out.println(graph.validate(new InvalidVertexIdsValidator<Long, String, Double>()));

        //Print on console the vertex and edge IDs, number of edges and vertices as well as the edge degree of the vertices!
        DataSet<Long> VertexIds = graph.getVertexIds();
        VertexIds.print();
        DataSet<Tuple2<Long, Long>> edgeIds = graph.getEdgeIds();
        edgeIds.print();
        //number edges and vertices
        System.out.println(graph.numberOfEdges());
        System.out.println(graph.numberOfVertices());

        //vertice degrees
        DataSet<Tuple2<Long, Long>> degrees = graph.getDegrees();
        degrees.print();
        
        // add a vertex and edges to your graph
        Graph<Long, String, Double> newGraph = graph.addVertex(new Vertex<Long, String>(6L, "Paris"))
        		.addEdge(new Vertex<Long, String>(6L, "Paris"), new Vertex<Long, String>(2L, "Berlin"), 1500.00D);        
        newGraph.getEdgesAsTuple3().print();

    }
}
