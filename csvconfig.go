// Copyright 2016 zxfonline@sina.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package csvconfig

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/zxfonline/fileutil"

	"github.com/zxfonline/golog"
)

var (
	logger = golog.New("csvconfig")
	//已经加载的文件缓存
	_tables map[string]*Table
	//文件路径
	pathPre     string
	defaultDirs []string
	//文件后缀
	filesuffix string
)

type Record struct {
	Fields map[string]string
}

type Table struct {
	Records []*Record
}

type Query struct {
	Key   string
	Value string
}

//初始化参数
func Init(pathpre, suffix string) {
	filesuffix = suffix
	if filesuffix == "" {
		filesuffix = ".csv"
	}
	pathpre = filepath.ToSlash(pathpre)
	pathPre = pathpre

	wd, _ := os.Getwd()
	arg0 := path.Clean(os.Args[0])
	var exeFile string
	if strings.HasPrefix(arg0, "/") {
		exeFile = arg0
	} else {
		exeFile = path.Join(wd, arg0)
	}
	parent, _ := path.Split(exeFile)
	defaultDirs = append(defaultDirs, path.Join(parent, "csv"))
	defaultDirs = append(defaultDirs, path.Join(wd, "csv"))
}

func findFile(table string) (*os.File, error) {
	if pathPre != "" {
		fpath := fileutil.PathJoin(pathPre, table+filesuffix)
		if fileutil.FileExists(fpath) {
			f, err := os.Open(fpath)
			if err != nil {
				return nil, fmt.Errorf("Open file error:%v ,path=%v", err, fpath)
			}
			return f, nil
		}
	} else {
		for _, dir := range defaultDirs {
			fpath := fileutil.PathJoin(dir, table+filesuffix)
			if fileutil.FileExists(fpath) {
				f, err := os.Open(fpath)
				if err != nil {
					return nil, fmt.Errorf("Open file error:%v ,path=%v", err, fpath)
				}
				return f, nil
			}
		}
	}
	return nil, fmt.Errorf("file no found,table=%v", table)
}

//加载所有配置文件，如果已经加载则覆盖
func Load(_table_list []string) error {
	_tables = make(map[string]*Table)
	for _, table := range _table_list {
		logger.Infof("Load csv config: %v", table)
		f, err := findFile(table)
		if err != nil {
			return err
		}
		defer f.Close()
		err = _initTable(table, f)
		if err != nil {
			return err
		}
	}
	return nil
}

func _initTable(table string, f *os.File) error {
	reader := csv.NewReader(f)
	title, err := reader.Read()
	if err != nil {
		return fmt.Errorf("parse csv config:%v ,error:%v", table, err)
	}
	for idx, val := range title {
		title[idx] = val
	}
	t := Table{Records: make([]*Record, 0)}
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if nil != err {
			return fmt.Errorf("parse csv config:%v ,error:%v", table, err)
		}

		rec := Record{Fields: make(map[string]string)}
		for idx, val := range line {
			rec.Fields[title[idx]] = val
		}
		t.Records = append(t.Records, &rec)
	}
	_tables[table] = &t
	return nil
}

func GetString(table string, queryField string, val string, field string) string {
	rec := getOne(table, queryField, val)
	if rec == nil {
		return ""
	}
	v, ok := rec.Fields[field]
	if !ok {
		return ""
	}
	return v
}

func getOne(tableName string, queryField string, val string) *Record {
	table, ok := _tables[tableName]
	if !ok {
		return nil
	}
	for _, rec := range table.Records {
		v, ok := rec.Fields[queryField]
		if !ok {
			return nil
		}
		if v == val {
			return rec
		}
	}
	return nil
}

func GetLines(table string, querys []*Query) []*Record {
	return getLines(table, querys)
}

func GetLine(table string, querys []*Query) *Record {
	ret := getLines(table, querys)
	if len(ret) > 0 {
		return ret[0]
	}
	return nil
}
func getLines(tableName string, querys []*Query) []*Record {
	table, ok := _tables[tableName]
	if !ok {
		return nil
	}
	var match bool
	lines := make([]*Record, 0)
	for _, rec := range table.Records {
		match = true
		for _, query := range querys {
			v, ok := rec.Fields[query.Key]
			if !ok {
				match = false
				break
			}
			if v != query.Value {
				match = false
				break
			}
		}
		if match {
			lines = append(lines, rec)
		}
	}
	return lines
}

func GetAll(tableName string) []*Record {
	table, ok := _tables[tableName]
	if !ok {
		return nil
	}
	lines := make([]*Record, len(table.Records))
	idx := 0
	for _, rec := range table.Records {
		lines[idx] = rec
		idx++
	}
	return lines
}
