package main

import (
	"fmt"
	mapreduce "mapreduce/mapreduce"
	"strings"
)

const numberOfWorkers = 10

type ExampleStruct struct {
	m map[string]int
}

func main() {
	map_ := func(emit mapreduce.EmitFunc[string, int], k int, v string, es *ExampleStruct) {
		for _, v := range strings.Split(v, " ") {
			if i, ok := es.m[v]; ok {
				es.m[v] = i + 1
			} else {
				es.m[v] = 1
			}
		}
	}
	reduce := func(emit mapreduce.EmitFunc[string, int], k string, vs []int) {
		sum := 0
		for _, v := range vs {
			sum += v
		}
		emit(k, sum)
	}
	mr := mapreduce.New(map_, reduce, numberOfWorkers)

	mr.SetInitMap(func() *ExampleStruct {
		m := make(map[string]int)
		return &ExampleStruct{m}
	})

	mr.SetFinalizeMap(func(emit mapreduce.EmitFunc[string, int], es *ExampleStruct) {
		for k, v := range es.m {
			emit(k, v)
		}
	})

	m := make(map[int]string)
	m[2013] = "de dag die je wist dat zou komen is eindelijk hier"
	m[1971] = "jaren komen en jaren gaan"
	m[1994] = "we komen en we gaan"

	r := mr.Run(m)

	fmt.Print(r)
}
