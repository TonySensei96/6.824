package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// Creates a hash map
	resultMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		// Gets the file name to decode
		mapFileName := reduceName(jobName, i, reduceTask)
		currMapFile, err := os.Open(mapFileName) // Open file
		defer currMapFile.Close()
		if err != nil {
			log.Fatal("Failed to open map task file:", mapFileName)
		}

		dec := json.NewDecoder(currMapFile)

		// Reads file decoder until the end of file
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv) // Gets the key/value from the decoder
			if err != nil {
				log.Fatal("Error occurs when decoding file:", mapFileName)
			}

			// Do the reduce task by appending the value string to the
			// according key in the hash map
			resultMap[kv.Key] = append(resultMap[kv.Key], kv.Value)
		}

		// Initializes a slice for key.
		// Initial len is 0, len up to the len of result hash map
		keySlice := make([]string, 0)

		for k := range resultMap {
			keySlice = append(keySlice, k)
		}

		sort.Strings(keySlice) // Sort by key

		// Creates an output file
		outFilePtr, err := os.Create(outFile)
		defer outFilePtr.Close()
		if err != nil {
			log.Fatal("Cannot open output file:", outFile)
		}
		enc := json.NewEncoder(outFilePtr)

		// Encodes everything from the hash map to a single result file
		for _, k := range keySlice {
			enc.Encode(KeyValue{k, reduceF(k, resultMap[k])})
		}

	}
}
