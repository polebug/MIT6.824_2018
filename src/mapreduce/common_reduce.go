package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

// doReduce() reads the intermediate files for the task,
// sorts the intermediate key/value pairs by key,
// calls the user-defined reduce function (reduceF) for each key,
// writes reduceF's output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string, // the application's reduce function
) {

	var (
		keyList   sort.StringSlice
		objMerged = make(map[string][]string)
	)

	// read one intermediate file from each map task
	for m := 0; m < nMap; m++ {
		imdFilename := reduceName(jobName, m, reduceTask)
		imdFile, err := os.Open(imdFilename)
		if err != nil {
			log.Printf("doReduce os.Open err = %v, filename = %s \n", err, imdFilename)
			continue
		}

		// doMap() encoded the key/value pairs in the intermediate files, so need to decode them.
		// get the values of each key for calling reduceF()
		dec := json.NewDecoder(imdFile)
		for true {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}

			keyList = append(keyList, kv.Key)
			objMerged[kv.Key] = append(objMerged[kv.Key], kv.Value)
		}

		imdFile.Close()
	}

	// sort the intermediate key/value pairs by key
	sort.Sort(keyList)

	// write the reduce output as JSON encoded KeyValue objects to the file named outFile
	file, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("doReduce os.Create err = %v, filename = %s \n", err, outFile)
	}

	enc := json.NewEncoder(file)

	for _, k := range keyList {
		// call reduceF() once per distinct key, with a slice of all the values for that key
		v := reduceF(k, objMerged[k])
		err = enc.Encode(KeyValue{
			Key:   k,
			Value: v,
		})
		if err != nil {
			log.Printf("doReduce enc.Encode err = %v, key = %v, value = %v \n", err, k, v)
			continue
		}
	}

	file.Close()
}
