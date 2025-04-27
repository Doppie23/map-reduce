package mapreduce

import "sync"

// TODO: InitMap and FinalizeMap

type EmitFunc[NK comparable, NV any] func(NK, NV)

type Mapper[K comparable, V any, NK comparable, NV any] func(EmitFunc[NK, NV], K, V)
type Reducer[NK comparable, NV any] func(EmitFunc[NK, NV], NK, []NV)

type MapReduce[K comparable, V any, NK comparable, NV any] struct {
	mapper  Mapper[K, V, NK, NV]
	reducer Reducer[NK, NV]
	workers int
}

func New[K comparable, V any, NK comparable, NV any](mapper Mapper[K, V, NK, NV], reducer Reducer[NK, NV], workers int) MapReduce[K, V, NK, NV] {
	return MapReduce[K, V, NK, NV]{
		mapper:  mapper,
		reducer: reducer,
		workers: workers,
	}
}

type SafeList[T any] struct {
	items []T
	mu    sync.Mutex
}

type Pair[K comparable, V any] struct {
	key   K
	value V
}

func (m MapReduce[K, V, NK, NV]) Run(dict map[K]V) map[NK]NV {
	var intermediate sync.Map
	emit := func(k NK, v NV) {
		l, _ := intermediate.LoadOrStore(k, &SafeList[NV]{})
		list := l.(*SafeList[NV])

		list.mu.Lock()
		list.items = append(list.items, v)
		list.mu.Unlock()
	}

	mapPairs := make([]Pair[K, V], 0, len(dict))
	for k, v := range dict {
		mapPairs = append(mapPairs, Pair[K, V]{k, v})
	}

	chunkSize := (len(mapPairs) + m.workers - 1) / m.workers
	var wg sync.WaitGroup

	for start := 0; start < len(mapPairs); start += chunkSize {
		end := min(start+chunkSize, len(mapPairs))
		wg.Add(1)

		go func(start int, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				pair := mapPairs[i]
				m.mapper(emit, pair.key, pair.value)
			}
		}(start, end)
	}

	wg.Wait()

	reducePairs := make([]Pair[NK, []NV], 0, len(dict))
	intermediate.Range(func(k any, v any) bool {
		reducePairs = append(reducePairs, Pair[NK, []NV]{k.(NK), v.(*SafeList[NV]).items})
		return true
	})

	chunkSize = (len(reducePairs) + m.workers - 1) / m.workers

	var reduceResult sync.Map
	emit = func(k NK, v NV) {
		// we dont really do anything about race conditions here
		// because each key should only be emitted once
		reduceResult.Store(k, v)
	}

	for start := 0; start < len(reducePairs); start += chunkSize {
		end := min(start+chunkSize, len(reducePairs))
		wg.Add(1)

		go func(start int, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				pair := reducePairs[i]
				m.reducer(emit, pair.key, pair.value)
			}
		}(start, end)
	}

	wg.Wait()

	result := make(map[NK]NV, len(dict))

	reduceResult.Range(func(k any, v any) bool {
		result[k.(NK)] = v.(NV)
		return true
	})

	return result
}
