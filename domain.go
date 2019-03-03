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

import "sync"

type Indexes map[string]interface{}

type Document struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	Id     string                 `json:"_id"`
	source map[string]interface{} `json:"_source"`
	Routing string 				  `json:"_routing"`
}

type Scroll struct {
	Took int `json:"took"`
	ScrollId string `json:"_scroll_id"`
	TimedOut bool   `json:"timed_out"`
	Hits     struct {
		     MaxScore float32    `json:"max_score"`
		     Total int           `json:"total"`
		     Docs  []interface{} `json:"hits"`
	     } `json:"hits"`
	Shards struct {
		     Total int `json:"total"`
		     Successful int `json:"successful"`
		     Failed int `json:"failed"`
		     Failures []struct {
			     Shard int     `json:"shard"`
			     Index string  `json:"index"`
			     Status int    `json:"status"`
			     Reason interface{} `json:"reason"`
		     } `json:"failures"`
	     } `json:"_shards"`
}

type ClusterVersion struct{
	Name   string `json:"name"`
	ClusterName   string `json:"cluster_name"`
	Version     struct {
			 Number string    `json:"number"`
			 LuceneVersion string    `json:"lucene_version"`
		 } `json:"version"`
}

type ClusterHealth struct {
	Name   string `json:"cluster_name"`
	Status string `json:"status"`
}

type Migrator struct{

	FlushLock       sync.Mutex
	DocChan         chan map[string]interface{}
	SourceESAPI     ESAPI
	TargetESAPI     ESAPI
	SourceAuth      *Auth
	TargetAuth      *Auth
	Config 		*Config
}


type Config struct {

	// config options
	SourceEs        string `short:"s" long:"source"  description:"source elasticsearch instance, ie: http://localhost:9200"`
	Query        string `short:"q" long:"query"  description:"query against source elasticsearch instance, filter data before migrate, ie: name:medcl"`
	TargetEs        string `short:"d" long:"dest"    description:"destination elasticsearch instance, ie: http://localhost:9201"`
	SourceEsAuthStr string `short:"m" long:"source_auth"  description:"basic auth of source elasticsearch instance, ie: user:pass"`
	TargetEsAuthStr  string `short:"n" long:"dest_auth"  description:"basic auth of target elasticsearch instance, ie: user:pass"`
	DocBufferCount    int    `short:"c" long:"count"   description:"number of documents at a time: ie \"size\" in the scroll request" default:"10000"`
	Workers           int    `short:"w" long:"workers" description:"concurrency number for bulk workers" default:"1"`
	BulkSizeInMB      int    `short:"b" long:"bulk_size" description:"bulk size in MB" default:"5"`
	ScrollTime        string `short:"t" long:"time"    description:"scroll time" default:"1m"`
	ScrollSliceSize   int    `long:"sliced_scroll_size"    description:"size of sliced scroll, to make it work, the size should be > 1" default:"1"`
	RecreateIndex     bool      `short:"f" long:"force"   description:"delete destination index before copying" default:"false"`
	CopyAllIndexes    bool   `short:"a" long:"all"     description:"copy indexes starting with . and _"`
	CopyIndexSettings bool   `long:"copy_settings"          description:"copy index settings from source" default:"false"`
	CopyIndexMappings bool   `long:"copy_mappings"          description:"copy index mappings from source" default:"false"`
	ShardsCount       int    `long:"shards"            description:"set a number of shards on newly created indexes"`
	SourceIndexNames  string `short:"x" long:"src_indexes" description:"indexes name to copy,support regex and comma separated list" default:"_all"`
	TargetIndexName   string `short:"y" long:"dest_index" description:"indexes name to save, allow only one indexname, original indexname will be used if not specified" default:""`
	WaitForGreen      bool   `long:"green"             description:"wait for both hosts cluster status to be green before dump. otherwise yellow is okay"`
	LogLevel          string `short:"v" long:"log"            description:"setting log level,options:trace,debug,info,warn,error"  default:"INFO"`
	DumpOutFile       string  `short:"o" long:"output_file"            description:"output documents of source index into local file" `
	DumpInputFile     string  `short:"i" long:"input_file"            description:"indexing from local dump file" `
	SourceProxy       string    `long:"source_proxy"            description:"set proxy to source http connections, ie: http://127.0.0.1:8080"`
	TargetProxy       string    `long:"dest_proxy"            description:"set proxy to target http connections, ie: http://127.0.0.1:8080"`
	Refresh           bool      `long:"refresh"                 description:"refresh after migration finished"`
	Fields            string `long:"fields"                 description:"output fields, comma separated, ie: col1,col2,col3,..." `

}

type Auth struct {
	User string
	Pass string
}
