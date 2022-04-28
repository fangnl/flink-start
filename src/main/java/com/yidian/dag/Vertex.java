package com.yidian.dag;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Vertex<T>
    implements Cloneable, Serializable {
  // ------------------------------------------------------------
  // Fields
  // ------------------------------------------------------------
  private final T label ;

  List<Vertex<T>> children = new ArrayList<>();

  List<Vertex<T>> parents = new ArrayList<>();

  // ------------------------------------------------------------
  // Constructors
  // ------------------------------------------------------------

  public Vertex(final T label) {
    this.label = label;
  }

  // ------------------------------------------------------------
  // Accessors
  // ------------------------------------------------------------

  public T getLabel() {
    return label;
  }

  public void addEdgeTo(final Vertex<T> vertex) {
    children.add(vertex);
  }

  public void removeEdgeTo(final Vertex<T> vertex) {
    children.remove(vertex);
  }

  public void addEdgeFrom(final Vertex<T> vertex) {
    parents.add(vertex);
  }

  public void removeEdgeFrom(final Vertex<T> vertex) {
    parents.remove(vertex);
  }

  public List<Vertex<T>> getChildren() {
    return children;
  }

  /**
   * Get the labels used by the most direct children.
   *
   * @return the labels used by the most direct children.
   */
  public List<T> getChildLabels() {
    final List<T> retValue = new ArrayList<>(children.size());

    for (Vertex<T> vertex : children) {
      retValue.add(vertex.getLabel());
    }
    return retValue;
  }

  /**
   * Get the list the most direct ancestors (parents).
   *
   * @return list of parents
   */
  public List<Vertex<T>> getParents() {
    return parents;
  }

  /**
   * Get the labels used by the most direct ancestors (parents).
   *
   * @return the labels used parents
   */
  public List<T> getParentLabels() {
    final List<T> retValue = new ArrayList<>(parents.size());

    for (Vertex<T> vertex : parents) {
      retValue.add(vertex.getLabel());
    }
    return retValue;
  }

  /**
   * Indicates if given vertex has no child
   *
   * @return <code>true</code> if this vertex has no child, <code>false</code> otherwise
   */
  public boolean isLeaf() {
    return children.size() == 0;
  }

  /**
   * Indicates if given vertex has no parent
   *
   * @return <code>true</code> if this vertex has no parent, <code>false</code> otherwise
   */
  public boolean isRoot() {
    return parents.size() == 0;
  }

  /**
   * Indicates if there is at least one edee leading to or from given vertex
   *
   * @return <code>true</code> if this vertex is connected with other vertex,<code>false</code> otherwise
   */
  public boolean isConnected() {
    return isRoot() || isLeaf();
  }

  @Override
  public Object clone()
      throws CloneNotSupportedException {
    // this is what's failing..
    final Object retValue = super.clone();

    return retValue;
  }

  @Override
  public String toString() {
    return "Vertex{" + "label='" + label + "'" + "}";
  }

}
