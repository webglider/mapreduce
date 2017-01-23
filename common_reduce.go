package mapreduce

import (
	"os"
	"encoding/json"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	
	// Read KeyValues from intermediate map output files
	kvs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		f, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			debug("doReduce(): %s", err)
			return
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue 
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}

	// Sort key value pairs by key
	sort.Sort(ByKey(kvs))

	// Aggregate values for each unique key
	akvs := make([]AggrKeyValue, 0)
	for i := 0; i < len(kvs); {
		j := i
		akv := AggrKeyValue {kvs[i].Key, make([]string, 0)}
		for ; j < len(kvs) && kvs[j].Key == kvs[i].Key; j++ {
			akv.Values = append(akv.Values, kvs[j].Value)
		}
		akvs = append(akvs, akv)
		i = j
	}

	// Apply the reduce function and write to output file
	of, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
		debug("doReduce(): %s", err)
		return
	}
	defer of.Close()

	enc := json.NewEncoder(of)
	for _, akv := range akvs {
		enc.Encode(&KeyValue{akv.Key, reduceF(akv.Key, akv.Values)})
	}
	
}
