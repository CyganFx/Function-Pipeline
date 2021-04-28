package main

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const th = 6

func SingleHash(in, out chan interface{}) {
	var mutex sync.Mutex
	var wg sync.WaitGroup

	for i := range in {
		wg.Add(1)
		go singleHashImpl(i, out, &wg, &mutex)
	}

	wg.Wait()
}

func singleHashImpl(in interface{}, out chan interface{}, wg *sync.WaitGroup, mutex *sync.Mutex) {
	defer wg.Done()

	var data, md5Data string

	if reflect.ValueOf(in).Type().String() == "int" {
		data = strconv.Itoa(in.(int))
	} else {
		data = in.(string)
	}

	mutex.Lock()
	{
		md5Data = DataSignerMd5(data)
	}
	mutex.Unlock()

	crc32DataChan := make(chan string)
	crc32Md5DataChan := make(chan string)

	go crc32Hasher(data, crc32DataChan)
	go crc32Hasher(md5Data, crc32Md5DataChan)

	crc32Data := <-crc32DataChan
	crc32Md5Data := <-crc32Md5DataChan

	result := crc32Data + "~" + crc32Md5Data
	out <- result

	fmt.Printf("%s SingleHash data %s\n", data, data)
	fmt.Printf("%s SingleHash md5(data) %s\n", data, md5Data)
	fmt.Printf("%s SingleHash crc32(md5(data)) %s\n", data, crc32Md5Data)
	fmt.Printf("%s SingleHash crc32(data) %s\n", data, crc32Data)
	fmt.Printf("%s SingleHash result %s\n", data, result)
}

func crc32Hasher(data string, out chan string) {
	select {
	case out <- DataSignerCrc32(data):
	case <-time.After(2 * time.Second):
		panic("hey, DataSignerCrc32 is taking too long, strange")
	}
}

func MultiHash(in, out chan interface{}) {
	var wg sync.WaitGroup

	for singleHash := range in {
		wg.Add(1)
		go multiHashImpl(singleHash.(string), out, &wg)
	}

	wg.Wait()
}

func multiHashImpl(singleHash string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	var localWg sync.WaitGroup

	localWg.Add(th)

	resultKeeper := make([]string, th)

	for i := 0; i < th; i++ {
		go func(idx int, localWg *sync.WaitGroup) {
			defer localWg.Done()

			thAndSingleHash := strconv.Itoa(idx) + singleHash
			crc32Data := DataSignerCrc32(thAndSingleHash)

			resultKeeper[idx] = crc32Data //thread safe writing, don't worry

		}(i, &localWg)
	}
	localWg.Wait()

	// can not be done in parrallel as it needs order
	for i := 0; i < th; i++ {
		fmt.Printf("%s MultiHash: crc32(th+step1)) %d %s\n", singleHash, i, resultKeeper[i])
	}

	result := strings.Join(resultKeeper, "")

	out <- result

	fmt.Printf("%s MultiHash result: %s\n", singleHash, result)
}

func CombineResults(in, out chan interface{}) {
	var multiHashResultsKeeper []string

	for multihashResult := range in {
		multiHashResultsKeeper = append(multiHashResultsKeeper, multihashResult.(string))
	}

	sort.Strings(multiHashResultsKeeper)

	res := strings.Join(multiHashResultsKeeper, "_")
	out <- res

	fmt.Printf("CombineResults %s\n", res)
}

func ExecutePipeline(jobs ...job) {
	var wg sync.WaitGroup
	wg.Add(len(jobs))

	inputChan := make(chan interface{})

	for _, concreteJob := range jobs {
		tempDataTransferChan := make(chan interface{}) //outbound

		go func(concreteJob job, in, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			defer close(out)

			concreteJob(in, out)

		}(concreteJob, inputChan, tempDataTransferChan, &wg)

		inputChan = tempDataTransferChan
		fmt.Println("Addresses:", inputChan, tempDataTransferChan)
	}

	wg.Wait()
}
