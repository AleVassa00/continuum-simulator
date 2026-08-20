package topology

import "sort"

type Assignment struct {
	SensorID    string
	MacroareaID string
	EdgeID      string
	Latitude    float64
	Longitude   float64
}

type Edge struct {
	ID          string
	MacroareaID string
	SensorCount int
}

type Index struct {
	assignmentsBySensor map[string]Assignment
	edges               map[string]Edge
	macroareas          map[string]bool
}

func (i *Index) SensorCount() int {
	return len(i.assignmentsBySensor)
}

func (i *Index) EdgeCount() int {
	return len(i.edges)
}

func (i *Index) MacroareaCount() int {
	return len(i.macroareas)
}

func (i *Index) Assignment(sensorID string) (Assignment, bool) {

	assignment, found := i.assignmentsBySensor[sensorID]

	return assignment, found
}

func (i *Index) Resolve(sensorID string) (string, string, bool) {

	assignment, found := i.Assignment(sensorID)

	if !found {
		return "", "", false
	}

	return assignment.EdgeID, assignment.MacroareaID, true
}

func (i *Index) Edge(id string) (Edge, bool) {

	edge, found := i.edges[id]

	return edge, found
}

func (i *Index) EdgeIDs() []string {

	ids := make([]string, 0, len(i.edges))

	for id := range i.edges {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}
