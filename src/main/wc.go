package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strconv"
	"strings"
	"unicode"
)

// mapF() splits the contents into words,
// and returns kv list about words count.
func mapF(
	filename string, // the input file
	contents string, // the file's complete contents
) (kvList []mapreduce.KeyValue) {
	// a word is any contiguous sequence of letters, as determined by unicode.IsLetter
	isLetter := func(c rune) bool {
		return !unicode.IsLetter(c)
	}
	words := strings.FieldsFunc(contents, isLetter)

	for _, word := range words {
		// for word count it only makes sense to use `words` as the keys.
		kvList = append(kvList, mapreduce.KeyValue{
			Key:   word,
			Value: "1",
		})
	}

	return kvList
}

// reduceF() will be called once for each key with a slice of all the values generated
// by mapF() for that key.
// It must return a string containing the total number of occurences of the key.
func reduceF(key string, values []string) string {
	return strconv.Itoa(len(values))
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
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
