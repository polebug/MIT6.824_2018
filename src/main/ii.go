package main

import (
	"fmt"
	"mapreduce"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// mapF() splits the contents into words,
// and returns kv list about word and it's file
func mapF(filename string, contents string) (res []mapreduce.KeyValue) {
	// a word is any contiguous sequence of letters, as determined by unicode.IsLetter
	isLetter := func(c rune) bool {
		return !unicode.IsLetter(c)
	}
	words := strings.FieldsFunc(contents, isLetter)

	for _, word := range words {
		// for word count it only makes sense to use `words` as the keys.
		// value is filename
		res = append(res, mapreduce.KeyValue{
			Key:   word,
			Value: filename,
		})
	}

	return res
}

// reduceF() count the files where the words are, need to remove duplicates
// an example of the return value:
// `ABOUT: 1 pg-tom_sawyer.txt`
func reduceF(key string, values []string) (res string) {
	var (
		filesmap = make(map[string]int) // use `map` to ensure value unique
		fileList sort.StringSlice       // use `slice` to sort
	)

	for _, v := range values {
		filesmap[v] = 1
	}

	for file := range filesmap {
		fileList = append(fileList, file)
	}
	sort.Sort(fileList)

	res = strconv.Itoa(len(fileList)) + " "
	for _, file := range fileList {
		res += file + ","
	}

	// remove the last `,`
	return strings.TrimRight(res, ",")
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
