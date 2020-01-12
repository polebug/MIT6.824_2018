package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// ihash: hash algorithm with key
func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

// doMap:
// read one of the input files(inFile), call the user-defined map function (mapF) for that file's contents,
// and partition mapF's output into nReduce intermediate files
func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string, // file name
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue, // convert file content to key/value pairs for reduce
) {

	// read input file, and get contents(byte), err
	inputContents, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatalf("doMap ioutil.ReadFile err = %v, filename = %s \n", err, inFile)
	}

	kvList := mapF(inFile, string(inputContents))

	// generat the intermediate file(mrtmp.xxx-0-0) for reduce task `r`, and use JSON to format it.
	for r := 0; r < nReduce; r++ {
		imdFile := reduceName(jobName, mapTask, r)

		file, err := os.Create(imdFile)
		if err != nil {
			log.Fatalf("doMap os.Create err = %v, imdFile = %s \n", err, imdFile)
		}

		enc := json.NewEncoder(file)
		for _, kv := range kvList {
			// Call ihash() on each key, mod nReduce, to pick r for a key/value pair.
			if ihash(kv.Key)%nReduce == r {
				err = enc.Encode(kv)
				if err != nil {
					log.Printf("doMap enc.Encode err = %v, key = %s, value = %v \n", err, kv.Key, kv.Value)
				}
			}
		}

		file.Close()
	}
}
