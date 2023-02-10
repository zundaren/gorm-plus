package tests

import (
	"context"
	"fmt"
	"github.com/acmestack/gorm-plus/gplus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strconv"
	"sync"
	"testing"
	"time"
)

func db() *gorm.DB {
	gormConfig := &gorm.Config{
		Logger:                                   &DbLog{},
		SkipDefaultTransaction:                   true,
		FullSaveAssociations:                     true,
		DisableAutomaticPing:                     true,
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              true,
		AllowGlobalUpdate:                        true,
		CreateBatchSize:                          1000,
	}

	url := "/Users/chenbo36/Downloads/ideaworkspace3/baidu/lottop-vpdc/cloud-ai/build/data/ai.db"
	d, err := gorm.Open(sqlite.Open(url), gormConfig)
	if err != nil {
		panic(err)
	}

	return d
}

func TestContext(t *testing.T) {
	gplus.Init(db())

	dao := gplus.NewBaseDao[PlatForm]()

	w, m := gplus.NewQuery[PlatForm]()
	w.IsNotNull(&m.Id)
	dao.Delete(w)
	dao.Db().Table("sqlite_sequence").UpdateColumn("seq", 0)

	var group sync.WaitGroup
	group.Add(20)
	for i := 1; i <= 20; i++ {
		go func(idx int) {
			defer group.Done()
			p := &PlatForm{Name: strconv.Itoa(idx)}
			if idx%2 == 0 {
				dao.Insert(p)
			} else {
				dao.WithCtx(ctx(p.Name)).Insert(p)
			}
		}(i)
	}
	group.Wait()

	dao.SelectById(2)
	dao.WithCtx(ctx("select2")).SelectById(2)

	w2, m2 := gplus.NewQuery[PlatForm]()
	w2.Eq(&m2.Name, "2")
	dao.SelectOne(w2)

	dao.WithCtx(ctx("3-3")).UpdateById(&PlatForm{Id: 3, Name: "3-3"})
	dao.UpdateById(&PlatForm{Id: 4, Name: "4-4"})

	plvo, _ := gplus.SelectListModel[PlatForm, PlatFormVO](dao.Db(), w2)
	for _, v := range plvo {
		fmt.Printf("%v\n", v)
	}

}

type DbLog struct {
}

func (d DbLog) LogMode(level logger.LogLevel) logger.Interface {
	return d
}

func (d DbLog) Info(ctx context.Context, s string, i ...interface{}) {
}

func (d DbLog) Warn(ctx context.Context, s string, i ...interface{}) {
}

func (d DbLog) Error(ctx context.Context, s string, i ...interface{}) {
}

func ctx(v string) context.Context {
	ccc := context.Background()
	return context.WithValue(ccc, "traceId", v)
}

func (d DbLog) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sql, rows := fc()
	fmt.Printf("traceId: %v sql: %v rows: %v\n", ctx.Value("traceId"), sql, rows)
}

type PlatForm struct {
	Id    int    `json:"id" gorm:"type:integer not null primary key autoincrement;PRIMARYKEY;"`
	Name  string `json:"name" gorm:"type:text not null;"`
	Ip    string `json:"ip" gorm:"type:text not null;"`
	State string `json:"state" gorm:"type:text not null default '0';"`
}

func (PlatForm) TableName() string {
	return "platform"
}

type PlatFormVO struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}
