// Copyright 2014 Square Inc.
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

type hooks interface {
	pre(obj interface{}, exec Executor) error
	post(obj interface{}, exec Executor) error
}

// PreDelete will be executed before the DELETE statement.
type PreDelete interface {
	PreDelete(Executor) error
}

// PostDelete will be executed after the DELETE statement.
type PostDelete interface {
	PostDelete(Executor) error
}

type deleteHooks struct{}

func (deleteHooks) pre(obj interface{}, exec Executor) error {
	if v, ok := obj.(PreDelete); ok {
		if err := v.PreDelete(exec); err != nil {
			return err
		}
	}
	return nil
}

func (deleteHooks) post(obj interface{}, exec Executor) error {
	if v, ok := obj.(PostDelete); ok {
		if err := v.PostDelete(exec); err != nil {
			return err
		}
	}
	return nil
}

// PostGet will be executed after the GET statement.
type PostGet interface {
	PostGet(Executor) error
}

type getHooks struct{}

func (getHooks) post(obj interface{}, exec Executor) error {
	if v, ok := obj.(PostGet); ok {
		return v.PostGet(exec)
	}
	return nil
}

// PreInsert will be executed before the INSERT statement.
type PreInsert interface {
	PreInsert(Executor) error
}

// PostInsert will be executed after the INSERT statement.
type PostInsert interface {
	PostInsert(Executor) error
}

type insertHooks struct{}

func (insertHooks) pre(obj interface{}, exec Executor) error {
	if v, ok := obj.(PreInsert); ok {
		if err := v.PreInsert(exec); err != nil {
			return err
		}
	}
	return nil
}

func (insertHooks) post(obj interface{}, exec Executor) error {
	if v, ok := obj.(PostInsert); ok {
		if err := v.PostInsert(exec); err != nil {
			return err
		}
	}
	return nil
}

// PreReplace will be executed before the REPLACE statement.
type PreReplace interface {
	PreReplace(Executor) error
}

// PostReplace will be executed after the REPLACE statement.
type PostReplace interface {
	PostReplace(Executor) error
}

type replaceHooks struct{}

func (replaceHooks) pre(obj interface{}, exec Executor) error {
	if v, ok := obj.(PreReplace); ok {
		if err := v.PreReplace(exec); err != nil {
			return err
		}
	}
	return nil
}

func (replaceHooks) post(obj interface{}, exec Executor) error {
	if v, ok := obj.(PostReplace); ok {
		if err := v.PostReplace(exec); err != nil {
			return err
		}
	}
	return nil
}

// PreUpdate will be executed before the UPDATE statement.
type PreUpdate interface {
	PreUpdate(Executor) error
}

// PostUpdate will be executed after the UPDATE statement.
type PostUpdate interface {
	PostUpdate(Executor) error
}

type updateHooks struct{}

func (updateHooks) pre(obj interface{}, exec Executor) error {
	if v, ok := obj.(PreUpdate); ok {
		return v.PreUpdate(exec)
	}
	return nil
}

func (updateHooks) post(obj interface{}, exec Executor) error {
	if v, ok := obj.(PostUpdate); ok {
		return v.PostUpdate(exec)
	}
	return nil
}

// PreUpsert will be executed before the INSERT ON DUPLICATE UPDATE
// statement.
type PreUpsert interface {
	PreUpsert(Executor) error
}

// PostUpsert will be executed after the INSERT ON DUPLICATE UPDATE
// statement.
type PostUpsert interface {
	PostUpsert(Executor) error
}

type upsertHooks struct{}

func (upsertHooks) pre(obj interface{}, exec Executor) error {
	if v, ok := obj.(PreUpsert); ok {
		if err := v.PreUpsert(exec); err != nil {
			return err
		}
	}
	return nil
}

func (upsertHooks) post(obj interface{}, exec Executor) error {
	if v, ok := obj.(PostUpsert); ok {
		if err := v.PostUpsert(exec); err != nil {
			return err
		}
	}
	return nil
}

// PreCommit will be executed before a squalor.Tx.Commit().
type PreCommit func(*Tx) error

// PostCommit will be executed after a squalor.Tx.Commit(). The hook is provided
// with the result of the commit. The hook itself cannot fail.
type PostCommit func(error)
