package com.yidian.dag;


import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DAG = Directed Acyclic Graph
 */
public class Dag<T>
    implements Cloneable, Serializable {
  // ------------------------------------------------------------
  // Fields
  // ------------------------------------------------------------
  /**
   * Nodes will be kept in two data structures at the same time for faster processing
   */
  /**
   * Maps vertex's label to vertex
   */
  private Map<T, Vertex<T>> vertexMap = new HashMap<>();

  /**
   * Conatin list of all vertices
   */
  private List<Vertex<T>> vertexList = new ArrayList<>();

  // ------------------------------------------------------------
  // Constructors
  // ------------------------------------------------------------


  public Dag() {
    super();
  }

  // ------------------------------------------------------------
  // Accessors
  // ------------------------------------------------------------

  /**
   * @return the vertices
   */
  public List<Vertex<T>> getVertices() {
    return vertexList;
  }

  /**
   * @return the vertices
   * @deprecated instead use {@link #getVertices()}
   */


  public Set<T> getLabels() {
    return vertexMap.keySet();
  }

  // ------------------------------------------------------------
  // Implementation
  // ------------------------------------------------------------

  /**
   * Adds vertex to DAG. If vertex of given label already exist in DAG no vertex is added
   *
   * @param label The label of the Vertex
   * @return New vertex if vertex of given label was not present in the DAG or existing vertex if vertex of given
   * label was already added to DAG
   */
  public Vertex<T> addVertex(final T label) {
    Vertex<T> retValue;

    // check if vertex is already in DAG
    if (vertexMap.containsKey(label)) {
      retValue = vertexMap.get(label);
    } else {
      retValue = new Vertex<>(label);

      vertexMap.put(label, retValue);

      vertexList.add(retValue);
    }

    return retValue;
  }

  public void addEdge(final T from, final T to)
      throws CycleDetectedException {
    final Vertex<T> v1 = addVertex(from);

    final Vertex<T> v2 = addVertex(to);

    addEdge(v1, v2);
  }

  public void addEdge(final Vertex<T> from, final Vertex<T> to)
      throws CycleDetectedException {

    from.addEdgeTo(to);

    to.addEdgeFrom(from);

    final List<T> cycle = CycleDetector.introducesCycle(to);

    if (cycle != null) {
      // remove edge which introduced cycle

      removeEdge(from, to);

      final String msg = "Edge between '" + from + "' and '" + to + "' introduces to cycle in the graph";

      throw new CycleDetectedException(msg,cycle);
    }
  }

  public void removeEdge(final T from, final T to) {
    final Vertex<T> v1 = addVertex(from);

    final Vertex<T> v2 = addVertex(to);

    removeEdge(v1, v2);
  }

  public void removeEdge(final Vertex<T> from, final Vertex<T> to) {
    from.removeEdgeTo(to);

    to.removeEdgeFrom(from);
  }

  public Vertex<T> getVertex(final T label) {

    return vertexMap.get(label);
  }

  public boolean hasEdge(final T label1, final T label2) {
    final Vertex<T> v1 = getVertex(label1);

    final Vertex<T> v2 = getVertex(label2);

    return v1.getChildren().contains(v2);

  }

  /**
   * @param label see name
   * @return the childs
   */
  public List<T> getChildLabels(final T label) {
    final Vertex<T> vertex = getVertex(label);

    return vertex.getChildLabels();
  }

  /**
   * @param label see name
   * @return the parents
   */
  public List<T> getParentLabels(final T label) {
    final Vertex<T> vertex = getVertex(label);

    return vertex.getParentLabels();
  }

  /**
   * @see Object#clone()
   */
  @Override
  public Object clone()
      throws CloneNotSupportedException {
    // this is what's failing..
    final Object retValue = super.clone();

    return retValue;
  }

  /**
   * Indicates if there is at least one edge leading to or from vertex of given label
   *
   * @param label the label
   * @return <code>true</code> if this vertex is connected with other vertex,<code>false</code> otherwise
   */
  public boolean isConnected(final T label) {
    final Vertex<T> vertex = getVertex(label);

    return vertex.isConnected();

  }


  public List<Vertex<T>> getAllLeaf(){
    return vertexList.stream().filter(Vertex::isLeaf).collect(Collectors.toList());
  }

  /**
   * Return the list of labels of successor in order decided by topological sort
   *
   * @param label The label of the vertex whose predecessors are searched
   * @return The list of labels. Returned list contains also the label passed as parameter to this method. This label
   * should always be the last item in the list.
   */
  public List<T> getSuccessorLabels(final T label) {
    final Vertex<T> vertex = getVertex(label);

    final List<T> retValue;

    // optimization.
    if (vertex.isLeaf()) {
      retValue = new ArrayList<>(1);

      retValue.add(label);
    } else {
      retValue = TopologicalSorter.sort(vertex);
    }

    return retValue;
  }

}
