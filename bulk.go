/*
Copyright 2016 Medcl (m AT medcl.net)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"sync"
	log "github.com/cihub/seelog"
	"encoding/json"
	"bytes"
	"gopkg.in/cheggaaa/pb.v1"
	"time"
)

func (c *Migrator) NewBulkWorker(docCount *int, pb *pb.ProgressBar, wg *sync.WaitGroup) {

	log.Debug("start es bulk worker")

	bulkItemSize := 0
	mainBuf := bytes.Buffer{}
	docBuf := bytes.Buffer{}
	docEnc := json.NewEncoder(&docBuf)

	READ_DOCS:
	for {
		select {
		case docI, open := <-c.DocChan:
			var err error
			log.Trace("read doc from channel,", docI)
		// this check is in case the document is an error with scroll stuff
			if status, ok := docI["status"]; ok {
				if status.(int) == 404 {
					log.Error("error: ", docI["response"])
					continue
				}
			}

		// sanity check
			for _, key := range []string{"_index", "_type", "_source", "_id"} {
				if _, ok := docI[key]; !ok {
					//json,_:=json.Marshal(docI)
					//log.Errorf("failed parsing document: %v", string(json))
					break READ_DOCS
				}
			}

			var tempDestIndexName string
			tempDestIndexName = docI["_index"].(string)

			if c.Config.TargetIndexName != "" {
				tempDestIndexName = c.Config.TargetIndexName
			}

			doc := Document{
				Index:  tempDestIndexName,
				Type:   docI["_type"].(string),
				source: docI["_source"].(map[string]interface{}),
				Id:     docI["_id"].(string),
			}

			// add doc "_routing"
			if _, ok := docI["_routing"]; ok {
				doc.Routing = docI["_routing"].(string)
			}
		// if channel is closed flush and gtfo
			if !open {
				goto WORKER_DONE
			}

		// sanity check
			if len(doc.Index) == 0 || len(doc.Id) == 0 || len(doc.Type) == 0 {
				log.Errorf("failed decoding document: %+v", doc)
				continue
			}

		// encode the doc and and the _source field for a bulk request
			post := map[string]Document{
				"create": doc,
			}
			if err = docEnc.Encode(post); err != nil {
				log.Error(err)
			}
			if err = docEnc.Encode(doc.source); err != nil {
				log.Error(err)
			}

		// if we approach the 100mb es limit, flush to es and reset mainBuf
			if mainBuf.Len() + docBuf.Len() > (c.Config.BulkSizeInMB * 1000000) {
				goto CLEAN_BUFFER
			}

		// append the doc to the main buffer
			mainBuf.Write(docBuf.Bytes())
		// reset for next document
			bulkItemSize++
			docBuf.Reset()
			(*docCount)++
		case <-time.After(time.Second * 5):
			log.Debug("5s no message input")
			goto CLEAN_BUFFER
		case <-time.After(time.Minute * 5):
			log.Warn("5m no message input, close worker")
			goto WORKER_DONE
		}

		goto READ_DOCS

		CLEAN_BUFFER:
		c.TargetESAPI.Bulk(&mainBuf)
		log.Trace("clean buffer, and execute bulk insert")
		pb.Add(bulkItemSize)
		bulkItemSize = 0

	}
	WORKER_DONE:
	if docBuf.Len() > 0 {
		mainBuf.Write(docBuf.Bytes())
		bulkItemSize++
	}
	c.TargetESAPI.Bulk(&mainBuf)
	log.Trace("bulk insert")
	pb.Add(bulkItemSize)
	bulkItemSize = 0
	wg.Done()
}
