package mapreduce

// TODO: multiple runners
// TODO: InitMap and FinalizeMap

type EmitFunc[NK comparable, NV any] func(NK, NV)

type Mapper[K comparable, V any, NK comparable, NV any] func(EmitFunc[NK, NV], K, V)
type Reducer[NK comparable, NV any] func(EmitFunc[NK, NV], NK, []NV)

type MapReduce[K comparable, V any, NK comparable, NV any] struct {
	mapper  Mapper[K, V, NK, NV]
	reducer Reducer[NK, NV]
}

func New[K comparable, V any, NK comparable, NV any](mapper Mapper[K, V, NK, NV], reducer Reducer[NK, NV]) MapReduce[K, V, NK, NV] {
	return MapReduce[K, V, NK, NV]{
		mapper:  mapper,
		reducer: reducer,
	}
}

func (m MapReduce[K, V, NK, NV]) Run(dict map[K]V) map[NK]NV {
	mapRes := make(map[NK][]NV)
	emit := func(k NK, v NV) {
		if _, ok := mapRes[k]; ok {
			mapRes[k] = append(mapRes[k], v)
		} else {
			mapRes[k] = []NV{v}
		}
	}

	for k, v := range dict {
		m.mapper(emit, k, v)
	}

	result := make(map[NK]NV)
	emit = func(k NK, v NV) {
		result[k] = v
	}

	for k, v := range mapRes {
		m.reducer(emit, k, v)
	}

	return result
}
