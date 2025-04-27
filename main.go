package main

import (
	"fmt"
	mr "mapreduce/mapreduce"
	"strings"
)

const numberOfWorkers = 10

func main() {
	map_ := func(emit mr.EmitFunc[string, int], k int, v string) {
		for _, v := range strings.Split(v, " ") {
			emit(v, 1)
		}
	}
	reduce := func(emit mr.EmitFunc[string, int], k string, v []int) {
		sum := len(v)
		emit(k, sum)
	}
	mr := mr.New(map_, reduce, numberOfWorkers)

	m := make(map[int]string)
	m[2013] = "de dag die je wist dat zou komen is eindelijk hier"
	m[1971] = "jaren komen en jaren gaan"
	m[1994] = "we komen en we gaan"

	r := mr.Run(m)

	fmt.Print(r)
}
