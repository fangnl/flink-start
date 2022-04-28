package com.yidian.dag;

/*
 * Copyright The Codehaus Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.*;

/**
 * @author <a href="michal.maczka@dimatics.com">Michal Maczka</a>
 */
public class CycleDetector {

  private final static Integer NOT_VISITED = 0;

  private final static Integer VISITING = 1;

  private final static Integer VISITED = 2;

  public static <T> List<T> hasCycle(final Dag<T> graph) {
    final List<Vertex<T>> vertices = graph.getVertices();

    final Map<Vertex<T>, Integer> vertexStateMap = new HashMap<>();

    List<T> retValue = null;

    for (Vertex<T> vertex : vertices) {
      if (isNotVisited(vertex, vertexStateMap)) {
        retValue = introducesCycle(vertex, vertexStateMap);

        if (retValue != null) {
          break;
        }
      }
    }

    return retValue;
  }

  /**
   * This method will be called when an edge leading to given vertex was added and we want to check if introduction of
   * this edge has not resulted in apparition of cycle in the graph
   *
   * @param vertex         the vertex
   * @param vertexStateMap the vertex Map
   * @return the found cycle
   */
  public static <T> List<T> introducesCycle(final Vertex<T> vertex,
                                        final Map<Vertex<T>, Integer> vertexStateMap) {
    final LinkedList<T> cycleStack = new LinkedList<>();

    final boolean hasCycle = dfsVisit(vertex, cycleStack, vertexStateMap);

    if (hasCycle) {
      // we have a situation like: [b, a, c, d, b, f, g, h].
      // Label of Vertex which introduced the cycle is at the first position in the list
      // We have to find second occurrence of this label and use its position in the list
      // for getting the sublist of vertex labels of cycle participants
      //
      // So in our case we are searching for [b, a, c, d, b]
      final T label = cycleStack.getFirst();

      final int pos = cycleStack.lastIndexOf(label);

      final List<T> cycle = cycleStack.subList(0, pos + 1);

      Collections.reverse(cycle);

      return cycle;
    }

    return null;
  }

  public static <T> List<T> introducesCycle(final Vertex<T> vertex) {
    final Map<Vertex<T>, Integer> vertexStateMap = new HashMap<>();

    return introducesCycle(vertex, vertexStateMap);
  }

  private static <T> boolean isNotVisited(final Vertex<T> vertex,
                                        final Map<Vertex<T>, Integer> vertexStateMap) {
    final Integer state = vertexStateMap.get(vertex);

    return (state == null) || NOT_VISITED.equals(state);
  }

  private static <T> boolean isVisiting(final Vertex<T> vertex,
                                        final Map<Vertex<T>, Integer> vertexStateMap) {
    final Integer state = vertexStateMap.get(vertex);

    return VISITING.equals(state);
  }

  private static <T> boolean dfsVisit(final Vertex<T> vertex, final LinkedList<T> cycle,
                                  final Map<Vertex<T>, Integer> vertexStateMap) {
    cycle.addFirst(vertex.getLabel());

    vertexStateMap.put(vertex, VISITING);

    for (Vertex<T> v : vertex.getChildren()) {
      if (isNotVisited(v, vertexStateMap)) {
        final boolean hasCycle = dfsVisit(v, cycle, vertexStateMap);

        if (hasCycle) {
          return true;
        }
      } else if (isVisiting(v, vertexStateMap)) {
        cycle.addFirst(v.getLabel());

        return true;
      }
    }
    vertexStateMap.put(vertex, VISITED);

    cycle.removeFirst();

    return false;
  }

}
