package schema

import (
	"github.com/linkingthing/gorest/resource"
)

var version = resource.APIVersion{
	Group:   "testing",
	Version: "v1",
}

type Cluster struct {
	resource.ResourceBase
	Name string
}

type Node struct {
	resource.ResourceBase
	Name string
}

func (c Node) GetParents() []resource.ResourceKind {
	return []resource.ResourceKind{Cluster{}}
}

type NameSpace struct {
	resource.ResourceBase
	Name string
}

func (c NameSpace) GetParents() []resource.ResourceKind {
	return []resource.ResourceKind{Cluster{}}
}

type Deployment struct {
	resource.ResourceBase
	Name string
}

func (c Deployment) GetParents() []resource.ResourceKind {
	return []resource.ResourceKind{NameSpace{}}
}

type DaemonSet struct {
	resource.ResourceBase
	Name string
}

func (c DaemonSet) GetParents() []resource.ResourceKind {
	return []resource.ResourceKind{NameSpace{}}
}

type StatefulSet struct {
	resource.ResourceBase
	Name string
}

func (c StatefulSet) GetParents() []resource.ResourceKind {
	return []resource.ResourceKind{NameSpace{}}
}

type Pod struct {
	resource.ResourceBase

	Name                  string
	Count                 uint32
	Annotations           map[string]string
	OtherInfo             OtherPodInfo
	OtherInfoSlice        []OtherPodInfo
	OtherInfoPointer      *OtherPodInfo
	OtherInfoPointerSlice []*OtherPodInfo
}

type OtherPodInfo struct {
	Name    string
	Numbers []uint32
}

func (c Pod) GetParents() []resource.ResourceKind {
	return []resource.ResourceKind{Deployment{}, DaemonSet{}, StatefulSet{}}
}

type Location struct {
	NodeName string `json:"nodeName"`
}

func (c Pod) GetActions() []resource.Action {
	return []resource.Action{
		resource.Action{
			Name:  "move",
			Input: &Location{},
		},
	}
}

func (c Pod) CreateDefaultResource() resource.Resource {
	return &Pod{
		Count: 20,
		OtherInfo: OtherPodInfo{
			Name:    "other",
			Numbers: []uint32{1, 2, 3},
		},
		OtherInfoSlice: []OtherPodInfo{
			OtherPodInfo{
				Name:    "other",
				Numbers: []uint32{1, 2, 3},
			},
		},
		OtherInfoPointer: &OtherPodInfo{
			Name:    "other",
			Numbers: []uint32{1, 2, 3},
		},
		OtherInfoPointerSlice: []*OtherPodInfo{
			&OtherPodInfo{
				Name:    "other",
				Numbers: []uint32{1, 2, 3},
			},
		},
	}
}

func createSchemaManager() *SchemaManager {
	mgr := NewSchemaManager()
	resourceKinds := []resource.ResourceKind{
		Cluster{},
		Node{},
		NameSpace{},
		Deployment{},
		StatefulSet{},
		DaemonSet{},
		Pod{},
	}
	for _, kind := range resourceKinds {
		err := mgr.Import(&version, kind, &resource.DumbHandler{})
		if err != nil {
			panic("test data isn't correct:" + err.Error())
		}
	}
	return mgr
}
