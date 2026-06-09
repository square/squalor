// Copyright 2026 Square Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package squalor

import (
	"reflect"
	"sort"
)

// BoundModelInfo contains read-only metadata about a model bound to a DB.
type BoundModelInfo struct {
	TableName     string
	ModelType     reflect.Type
	MappedColumns []BoundColumnInfo
	PrimaryKey    BoundKeyInfo
	Indexes       []BoundKeyInfo
}

// BoundColumnInfo contains read-only metadata about a mapped database column.
type BoundColumnInfo struct {
	Name     string
	AutoIncr bool
	Nullable bool
	SQLType  string
}

// BoundKeyInfo contains read-only metadata about a database key or index.
type BoundKeyInfo struct {
	Name    string
	Primary bool
	Unique  bool
	Columns []string
}

// BoundModels returns read-only metadata snapshots for all models bound to db.
func (db *DB) BoundModels() []BoundModelInfo {
	db.mu.RLock()
	defer db.mu.RUnlock()

	models := make([]BoundModelInfo, 0, len(db.models))
	for modelType, model := range db.models {
		models = append(models, boundModelInfo(modelType, model))
	}

	sort.Slice(models, func(i, j int) bool {
		if models[i].TableName != models[j].TableName {
			return models[i].TableName < models[j].TableName
		}
		return models[i].ModelType.String() < models[j].ModelType.String()
	})
	return models
}

func boundModelInfo(modelType reflect.Type, model *Model) BoundModelInfo {
	info := BoundModelInfo{
		TableName:     model.Name,
		ModelType:     modelType,
		MappedColumns: boundColumnInfos(model.mappedColumns),
		PrimaryKey:    boundKeyInfo(model.PrimaryKey),
		Indexes:       boundKeyInfos(model.Keys),
	}
	return info
}

func boundColumnInfos(columns []*Column) []BoundColumnInfo {
	infos := make([]BoundColumnInfo, len(columns))
	for i, column := range columns {
		infos[i] = boundColumnInfo(column)
	}
	return infos
}

func boundColumnInfo(column *Column) BoundColumnInfo {
	if column == nil {
		return BoundColumnInfo{}
	}
	return BoundColumnInfo{
		Name:     column.Name,
		AutoIncr: column.AutoIncr,
		Nullable: column.Nullable,
		SQLType:  column.sqlType,
	}
}

func boundKeyInfos(keys []*Key) []BoundKeyInfo {
	infos := make([]BoundKeyInfo, len(keys))
	for i, key := range keys {
		infos[i] = boundKeyInfo(key)
	}
	return infos
}

func boundKeyInfo(key *Key) BoundKeyInfo {
	if key == nil {
		return BoundKeyInfo{}
	}
	info := BoundKeyInfo{
		Name:    key.Name,
		Primary: key.Primary,
		Unique:  key.Unique,
		Columns: make([]string, len(key.Columns)),
	}
	for i, column := range key.Columns {
		if column != nil {
			info.Columns[i] = column.Name
		}
	}
	return info
}
