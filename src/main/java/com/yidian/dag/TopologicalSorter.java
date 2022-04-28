package com.yidian.dag;


import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class TopologicalSorter {

  private final static Integer NOT_VISITED = 0;

  private final static Integer VISITING = 1;

  private final static Integer VISITED = 2;

  /**
   * @param graph the graph
   * @return List of String (vertex labels)
   */
  public   static <T> List<T> sort(final Dag<T> graph) {
    return dfs(graph);
  }

  public static <T> List<T> sort(final Vertex<T> vertex) {
    // we need to use addFirst method so we will use LinkedList explicitly
    final List<T> retValue = new LinkedList<>();

    dfsVisit(vertex, new HashMap<>(), retValue);

    return retValue;
  }

  private static <T> List<T> dfs(final Dag<T> graph) {
    // we need to use addFirst method so we will use LinkedList explicitly
    final List<T> retValue = new LinkedList<>();
    final Map<Vertex<T>, Integer> vertexStateMap = new HashMap<>();

    for (Vertex<T> vertex : graph.getVertices()) {
      if (isNotVisited(vertex, vertexStateMap)) {
        dfsVisit(vertex, vertexStateMap, retValue);
      }
    }

    return retValue;
  }

  private static <T> boolean isNotVisited(final Vertex<T> vertex, final Map<Vertex<T>, Integer> vertexStateMap) {
    final Integer state = vertexStateMap.get(vertex);

    return (state == null) || NOT_VISITED.equals(state);
  }

  private static <T> void dfsVisit(final Vertex<T> vertex, final Map<Vertex<T>, Integer> vertexStateMap,
                               final List<T> list) {
    vertexStateMap.put(vertex, VISITING);

    for (Vertex<T> v : vertex.getChildren()) {
      if (isNotVisited(v, vertexStateMap)) {
        dfsVisit(v, vertexStateMap, list);
      }
    }

    vertexStateMap.put(vertex, VISITED);

    list.add(vertex.getLabel());
  }

}
