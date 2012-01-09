// ----------------------
// projects/graph/Graph.h
// Copyright (C) 2011
// Glenn P. Downing
// ----------------------

#ifndef Graph_h
#define Graph_h

// --------
// includes
// --------

#include <cassert> // assert
#include <cstddef> // size_t
#include <utility> // make_pair, pair
#include <vector> // vector
// -----
// Graph
// -----

class Graph {
public:
	// --------
	// typedefs
	// --------

	typedef unsigned int vertex_descriptor;
	typedef std::pair<vertex_descriptor, vertex_descriptor> edge_descriptor;

	typedef std::vector<vertex_descriptor>::const_iterator vertex_iterator;
	typedef std::vector<edge_descriptor>::const_iterator edge_iterator;
	typedef std::vector<vertex_descriptor>::const_iterator adjacency_iterator;

	typedef std::size_t vertices_size_type;
	typedef std::size_t edges_size_type;

public:
	// --------
	// add_edge
	// --------

	/**
	 * @param vd1 the source vertex for this edge
	 * @param vd2 the target vertex for this edge
	 * @param graph the graph vd1 and vd2 belong to
	 * @return a pair containing the edge_descriptor as its first element and a bool as its second element
	 * the bool is true if this edge was just added
	 * @throws invalid_argument if vertex_descriptor not valid
	 * @throws invalid_argument if edge exists
	 */
	friend std::pair<edge_descriptor, bool> add_edge(vertex_descriptor vd1,
			vertex_descriptor vd2, Graph& graph) {

		std::pair<edge_descriptor, bool> p = edge(vd1, vd2, graph);
		edge_descriptor ed;
		bool b;
		if (p.second) {
			ed = p.first;
			b = false;
		} else {
			graph.g[vd1].push_back(vd2);
			ed = std::make_pair(vd1, vd2);
			graph.elist.push_back(ed);
			b = true;
		}
		return std::make_pair(ed, b);
	}

	// ----------
	// add_vertex
	// ----------

	/**
	 * @param graph is the graph a new vertex will be added to
	 * @return a vertex_descriptor for the added vertex
	 */
	friend vertex_descriptor add_vertex(Graph& graph) {
		vertex_descriptor vd = graph.g.size();
		graph.vlist.push_back(vd);
		std::vector<vertex_descriptor> v;
		graph.g.push_back(v);
		return vd;
	}

	// -----------------
	// adjacent_vertices
	// -----------------

	/**
	 * @param vd a vertex_descriptor in graph whose adjacent vertices are requested
	 * @param graph the graph vd belongs to
	 * @return a pair containing an iterator to the beginning and end of the adjacent vertices of vd
	 * @throws invalid_argument if vertex_descriptor not valid
	 */
	friend std::pair<adjacency_iterator, adjacency_iterator> adjacent_vertices(
			vertex_descriptor vd, const Graph& graph) {
		adjacency_iterator b = graph.g[vd].begin();
		adjacency_iterator e = graph.g[vd].end();
		return std::make_pair(b, e);
	}

	// ----
	// edge
	// ----

	/**
	 * @param vd1 the source vertex for this edge
	 * @param vd2 the target vertex for this edge
	 * @param graph the graph containing vd1 and vd2
	 * @return a pair containing an edge_descriptor between vd1 and vd2 and a bool to signify if the edge was added
	 * @throws invalid_argument if vertex_descriptor not valid
	 */
	friend std::pair<edge_descriptor, bool> edge(vertex_descriptor vd1,
			vertex_descriptor vd2, const Graph& graph) {
		edge_descriptor ed = std::make_pair(0, 0);
		bool b = false;
		if (vd1 < graph.g.size()) {
			if (vd2 < graph.g[vd1].size()) {
				ed = std::make_pair(vd1, vd2);
				b = true;
			}
		}
		return std::make_pair(ed, b);
	}

	// -----
	// edges
	// -----

	/**
	 * O(1) in space
	 * O(1) in time
	 * <your documentation>
	 */
	friend std::pair<edge_iterator, edge_iterator> edges(const Graph& graph) {
		// <your code>
		edge_iterator b = graph.elist.begin();
		edge_iterator e = graph.elist.end();
		return std::make_pair(b, e);
	}

	// ---------
	// num_edges
	// ---------

	/**
	 * @param graph the graph in question
	 * @return the number of edges in graph
	 */
	friend edges_size_type num_edges(const Graph& graph) {
		return graph.elist.size();
	}

	// ------------
	// num_vertices
	// ------------

	/**
	 * @param graph the graph in question
	 * @return the number of vertices in graph
	 */
	friend vertices_size_type num_vertices(const Graph& graph) {
		return graph.vlist.size();
	}

	// ------
	// source
	// ------

	/**
	 * @param ed an edge_descriptor
	 * @param graph the graph ed belongs to
	 * @return the source of ed
	 * @throws invalid_argument if edge_descriptor not valid
	 */
	friend vertex_descriptor source(edge_descriptor ed, const Graph& graph) {
		assert (ed.first < graph.g.size());
		return ed.first;
	}

	// ------
	// target
	// ------

	/**
	 * @param ed an edge_descriptor
	 * @param graph the graph ed belongs to
	 * @return the target of ed
	 * @throws invalid_argument if edge_descriptor not valid
	 */
	friend vertex_descriptor target(edge_descriptor ed, const Graph& graph) {
		assert (ed.first < graph.g.size());
		assert (ed.second < graph.g.size());
		return ed.second;
	}

	// ------
	// vertex
	// ------

	/**
	 * @param v
	 * @param graph the graph v belongs to
	 * @throws invalid_argument if vertices_size_type not valid
	 */
	friend vertex_descriptor vertex(vertices_size_type v, const Graph& graph) {
		assert (v < graph.g.size());
		return v;
	}

	// --------
	// vertices
	// --------

	/**
	 * @param graph
	 * @return a pair of iterators to the beginning and end of the vertices in graph
	 */
	friend std::pair<vertex_iterator, vertex_iterator> vertices(
			const Graph& graph) {
		vertex_iterator b = graph.vlist.begin();
		vertex_iterator e = graph.vlist.end();
		return std::make_pair(b, e);
	}

private:
	// ----
	// data
	// ----

	std::vector<vertex_descriptor> vlist;
	std::vector<edge_descriptor> elist;
	std::vector<std::vector<vertex_descriptor> > g; // something like this

	// -----
	// valid
	// -----

	/**
	 * <your documentation>
	 */
	bool valid() const {
		bool val = true;
		if (vlist.size() < 0 || elist.size() < 0)
			val = false;
		return val;
	}

public:
	// ------------
	// constructors
	// ------------

	/**
	 * <your documentation>
	 */
	Graph() {
		assert(valid());
	}

	// Default copy, destructor, and copy assignment
	// Graph (const Graph<T>&);
	// ~Graph ();
	// Graph& operator = (const Graph&);
};

// ---------
// has_cycle
// ---------

/**
 * depth-first traversal
 * three colors
 *
 * @param g is the graph to check
 * @return bool true if g has a cycle, false otherwise
 */
template<typename G>
bool has_cycle(const G& g) {

	typedef typename G::vertex_descriptor vertex_descriptor;
	typedef typename G::vertex_iterator vertex_iterator;

	std::set<vertex_descriptor> grey_list;
	std::set<vertex_descriptor> black_list;

	std::pair<vertex_iterator, vertex_iterator> p = vertices(g);
	vertex_iterator b = p.first;
	vertex_iterator e = p.second;

	bool r = false;
	while (b != e && !r) {
		if (black_list.find(*b) == black_list.end())
			r = has_cycle_helper(*b, grey_list, black_list, g);
		++b;
	}
	return r;
}

/**
 * helper method for has_cycle
 * recursively checks node vd and its children for a grey node
 * if a grey node is found, we have a cycle and has_cycle_helper returns true
 *
 * @param vd is the vertex to check
 * @param grey_list is a reference to the list of grey nodes
 * @param black_list is a reference to the list of black nodes
 * @return true if vd or one of its children is grey, false otherwise
 */
template<typename G>
bool has_cycle_helper(typename G::vertex_descriptor vd,
		std::set<typename G::vertex_descriptor>& grey_list,
		std::set<typename G::vertex_descriptor>& black_list, const G& g) {

	std::pair<typename G::adjacency_iterator, typename G::adjacency_iterator>
			p = adjacent_vertices(vd, g);
	typename G::adjacency_iterator b = p.first;
	typename G::adjacency_iterator e = p.second;

	bool r = false;
	if (grey_list.find(vd) == grey_list.end()) {
		grey_list.insert(vd);
		while (b != e && !r) {
			if (black_list.find(*b) == black_list.end())
				r = has_cycle_helper(*b, grey_list, black_list, g);
			++b;
		}
		if (!r) {
			grey_list.erase(vd);
			black_list.insert(vd);
		}
	} else {
		r = true;
	}

	return r;
}

// ----------------
// topological_sort
// ----------------

/**
 * depth-first traversal
 * calls tsort_helper with all nodes that have no incoming vertices
 * @param g is the graph to check
 * @param x is the output iterator to write the sorted results to
 * @throws invalid_argument if !has_cycle(g)
 */
template<typename G, typename OI>
void topological_sort(const G& g, OI x) {

	typedef typename G::vertex_descriptor vertex_descriptor;
	typedef typename G::vertex_iterator vertex_iterator;
	typedef typename G::edge_descriptor edge_descriptor;
	typedef typename G::edge_iterator edge_iterator;

	assert(!has_cycle(g));

	std::vector<vertex_descriptor> sorted; //list that will contain the sorted nodes (will be in reverse order)
	std::set<vertex_descriptor> roots; //all nodes with no incoming edges
	std::set<vertex_descriptor> visited;

	std::pair<vertex_iterator, vertex_iterator> p1 =
			vertices(g);
	std::pair<edge_iterator, edge_iterator> p2 = edges(
			g);
	vertex_iterator b1 = p1.first;
	vertex_iterator e1 = p1.second;
	edge_iterator b2 = p2.first;
	edge_iterator e2 = p2.second;

	while (b1 != e1) {
		roots.insert(*b1);
		++b1;
	}
	while (b2 != e2) {
		roots.erase(target(*b2, g));
		++b2;
	}

	typename std::set<vertex_descriptor, std::less<vertex_descriptor>,
			std::allocator<vertex_descriptor> >::iterator b3 = roots.begin();
	typename std::set<vertex_descriptor, std::less<vertex_descriptor>,
			std::allocator<vertex_descriptor> >::iterator e3 = roots.end();

	while (b3 != e3) {
		tsort_helper(*b3, sorted, visited, g);
		++b3;
	}

	while (!sorted.empty()) {
		*x = sorted.back();
		sorted.pop_back();
		++x;
	}
}

/**
 * checks individual vertices
 * if a vertex has been visited, do nothing
 * otherwise, mark the vertex as visited, and recursively call tsort_helper on each
 * vertex this vd points to.
 * after all child vertices have been visited, add vd to the sorted list
 * @param vd is the vertex to check
 * @param sorted is a vector of vertices in reverse sorted order
 * @param visited is the list of vertices that have been visited
 * @param g is the graph vd belongs to
 */
template<typename G>
void tsort_helper(typename G::vertex_descriptor vd,
		std::vector<typename G::vertex_descriptor>& sorted,
		std::set<typename G::vertex_descriptor>& visited, const G& g) {

	if (visited.find(vd) == visited.end()) {
		visited.insert(vd);

		std::pair<typename G::adjacency_iterator,
				typename G::adjacency_iterator> p = adjacent_vertices(vd, g);
		typename G::adjacency_iterator b = p.first;
		typename G::adjacency_iterator e = p.second;

		while (b != e) {
			tsort_helper(*b, sorted, visited, g);
			++b;
		}
		sorted.push_back(vd);
	}
}

#endif // Graph_h
