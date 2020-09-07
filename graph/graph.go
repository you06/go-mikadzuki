package graph

type Graph struct {
	allocID    int
	timelines  []Timeline
	dependency int
}

func NewGraph() Graph {
	return Graph{
		allocID:    0,
		timelines:  []Timeline{},
		dependency: 0,
	}
}

func (g *Graph) NewTimeline() *Timeline {
	id := g.allocID
	g.allocID += 1
	g.timelines = append(g.timelines, NewTimeline(id))
	return &g.timelines[id]
}

func (g *Graph) GetTimeline(n int) *Timeline {
	if n < g.allocID {
		return &g.timelines[n]
	}
	return nil
}

func (g *Graph) AddDependency(dependTp DependTp) {

}
