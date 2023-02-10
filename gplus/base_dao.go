/*
 * Licensed to the AcmeStack under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gplus

import (
	"context"
	"github.com/acmestack/gorm-plus/constants"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"gorm.io/gorm/utils"
	"reflect"
)

var gormDb *gorm.DB
var defaultBatchSize = 1000

func Init(db *gorm.DB) {
	gormDb = db
}

type BaseDao[T any] struct {
	db *gorm.DB
}

func NewBaseDao[T any]() *BaseDao[T] {
	return &BaseDao[T]{
		db: gormDb,
	}
}

type Page[T any] struct {
	Current int
	Size    int
	Total   int64
	Records []*T
}

func NewPage[T any](current, size int) *Page[T] {
	return &Page[T]{Current: current, Size: size}
}

func (d BaseDao[T]) Db() *gorm.DB {
	return d.db
}

func (d BaseDao[T]) WithCtx(ctx context.Context) BaseDao[T] {
	return BaseDao[T]{db: d.db.WithContext(ctx)}
}

func (d BaseDao[T]) Insert(entity *T) *gorm.DB {
	return d.db.Create(entity)
}

func (d BaseDao[T]) InsertBatch(entities []*T) *gorm.DB {
	if len(entities) == 0 {
		return gormDb
	}
	return d.db.CreateInBatches(entities, defaultBatchSize)
}

func (d BaseDao[T]) InsertBatchSize(entities []*T, batchSize int) *gorm.DB {
	if len(entities) == 0 {
		return gormDb
	}
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return d.db.CreateInBatches(entities, batchSize)
}

func (d BaseDao[T]) DeleteById(id any) *gorm.DB {
	var entity T
	return d.db.Where(getPkColumnName[T](), id).Delete(&entity)
}

func (d BaseDao[T]) DeleteByIds(ids any) *gorm.DB {
	q, _ := NewQuery[T]()
	q.In(getPkColumnName[T](), ids)
	return d.Delete(q)
}

func (d BaseDao[T]) Delete(q *Query[T]) *gorm.DB {
	var entity T
	return d.db.Where(q.QueryBuilder.String(), q.QueryArgs...).Delete(&entity)
}

func (d BaseDao[T]) UpdateById(entity *T) *gorm.DB {
	return d.db.Model(entity).Updates(entity)
}

func (d BaseDao[T]) Update(q *Query[T]) *gorm.DB {
	return d.db.Model(new(T)).
		Where(q.QueryBuilder.String(), q.QueryArgs...).
		Updates(&q.UpdateMap)
}

func (d BaseDao[T]) SelectById(id any) (*T, *gorm.DB) {
	q, _ := NewQuery[T]()
	q.Eq(getPkColumnName[T](), id)
	var entity T
	resultDb := buildCondition(d.db, q)
	return &entity, resultDb.Limit(1).Find(&entity)
}

func (d BaseDao[T]) SelectByIds(ids any) ([]*T, *gorm.DB) {
	q, _ := NewQuery[T]()
	q.In(getPkColumnName[T](), ids)
	return d.SelectList(q)
}

func (d BaseDao[T]) SelectOne(q *Query[T]) (*T, *gorm.DB) {
	var entity T
	resultDb := buildCondition(d.db, q)
	return &entity, resultDb.Limit(1).Find(&entity)
}

func (d BaseDao[T]) SelectList(q *Query[T]) ([]*T, *gorm.DB) {
	resultDb := buildCondition(d.db, q)
	var results []*T
	resultDb.Find(&results)
	return results, resultDb
}

func (d BaseDao[T]) SelectPage(page *Page[T], q *Query[T]) (*Page[T], *gorm.DB) {
	total, countDb := d.SelectCount(q)
	if countDb.Error != nil {
		return page, countDb
	}
	page.Total = total
	resultDb := buildCondition(d.db, q)
	var results []*T
	resultDb.Scopes(paginate(page)).Find(&results)
	page.Records = results
	return page, resultDb
}

func (d BaseDao[T]) SelectCount(q *Query[T]) (int64, *gorm.DB) {
	var count int64
	resultDb := buildCondition(d.db, q)
	resultDb.Count(&count)
	return count, resultDb
}

func SelectListModel[T any, R any](db *gorm.DB, q *Query[T]) ([]*R, *gorm.DB) {
	resultDb := buildCondition(db, q)
	var results []*R
	resultDb.Scan(&results)
	return results, resultDb
}

func SelectPageModel[T any, R any](db *gorm.DB, page *Page[R], q *Query[T]) (*Page[R], *gorm.DB) {
	var total int64
	countDb := buildCondition(db, q).Count(&total)
	if countDb.Error != nil {
		return page, countDb
	}
	page.Total = total
	resultDb := buildCondition(db, q)
	var results []*R
	resultDb.Scopes(paginate(page)).Scan(&results)
	page.Records = results
	return page, resultDb
}

func paginate[T any](p *Page[T]) func(db *gorm.DB) *gorm.DB {
	page := p.Current
	pageSize := p.Size
	return func(db *gorm.DB) *gorm.DB {
		if page <= 0 {
			page = 1
		}
		if pageSize <= 0 {
			pageSize = 10
		}
		offset := (page - 1) * pageSize
		return db.Offset(offset).Limit(pageSize)
	}
}

func buildCondition[T any](db *gorm.DB, q *Query[T]) *gorm.DB {
	resultDb := db.Model(new(T))
	if q != nil {
		if len(q.DistinctColumns) > 0 {
			resultDb.Distinct(q.DistinctColumns)
		}

		if len(q.SelectColumns) > 0 {
			resultDb.Select(q.SelectColumns)
		}

		if q.QueryBuilder.Len() > 0 {

			if q.AndBracketBuilder.Len() > 0 {
				q.QueryArgs = append(q.QueryArgs, q.AndBracketArgs...)
				q.QueryBuilder.WriteString(q.AndBracketBuilder.String())
			}

			if q.OrBracketBuilder.Len() > 0 {
				q.QueryArgs = append(q.QueryArgs, q.OrBracketArgs...)
				q.QueryBuilder.WriteString(q.OrBracketBuilder.String())
			}

			resultDb.Where(q.QueryBuilder.String(), q.QueryArgs...)
		}

		if q.OrderBuilder.Len() > 0 {
			resultDb.Order(q.OrderBuilder.String())
		}

		if q.GroupBuilder.Len() > 0 {
			resultDb.Group(q.GroupBuilder.String())
		}

		if q.HavingBuilder.Len() > 0 {
			resultDb.Having(q.HavingBuilder.String(), q.HavingArgs...)
		}
	}
	return resultDb
}

func getPkColumnName[T any]() string {
	var entity T
	entityType := reflect.TypeOf(entity)
	numField := entityType.NumField()
	var columnName string
	for i := 0; i < numField; i++ {
		field := entityType.Field(i)
		tagSetting := schema.ParseTagSetting(field.Tag.Get("gorm"), ";")
		isPrimaryKey := utils.CheckTruth(tagSetting["PRIMARYKEY"], tagSetting["PRIMARY_KEY"])
		if isPrimaryKey {
			name, ok := tagSetting["COLUMN"]
			if !ok {
				namingStrategy := schema.NamingStrategy{}
				name = namingStrategy.ColumnName("", field.Name)
			}
			columnName = name
			break
		}
	}
	if columnName == "" {
		return constants.DefaultPrimaryName
	}
	return columnName
}
