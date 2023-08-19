package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

/*
migrate 是 go 数据迁移工具，可以注入处理器或处理单元；
需要指定 db 连接和概要表 schemaTable，用于存储已执行的索引；
顺序执行处理单元，对错误进行返回。
*/

const (
	defaultSchemaTableName = "schema_migrations"
)

const (
	ErrDuplicateIndexFormat = "duplicate index is %d"
	ErrIndexGapLargeFormat  = "index gap is larger than 1, current index is %d"
	ErrFindIndexDirtyFormat = "find dirty index %d"
)

const (
	createSchemaTableQuery = "CREATE TABLE IF NOT EXISTS %s (`version` int NOT NULL DEFAULT 0, `dirty` tinyint(1) NOT NULL DEFAULT 1) ENGINE=InnoDB;"

	selectSchemaQuery = "SELECT * FROM %s"

	updateSchemaQuery = "UPDATE %s SET `version` = ?"

	updateDirtyQuery = "UPDATE %s SET `version` = ?, `dirty` = ?"

	insertDefaultSchema = "INSERT INTO %s (`version`, `dirty`) VALUES (0, 0)"
)

var (
	ErrIndexLessDatabaseVersion = errors.New("index less than database version")
)

type Migrate interface {
	AddExecutors(executors ...Executor)
	AddHandlers(handlers ...Handler)

	Run(ctx context.Context) error
}

type migrate struct {
	mutex sync.Mutex

	db          *sql.DB // db 连接
	schemaTable string  // 概要表，记录当前执行位置

	executors []Executor // 运行器列表
	handlers  []Handler  // 运行单元列表
}

func New(db *sql.DB, options ...Option) Migrate {
	migrate := migrate{
		db:          db,
		schemaTable: defaultSchemaTableName,
	}
	for _, option := range options {
		option(&migrate)
	}
	return &migrate
}

func (m *migrate) AddExecutors(executors ...Executor) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.executors = append(m.executors, executors...)
}

func (m *migrate) AddHandlers(handlers ...Handler) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.handlers = append(m.handlers, handlers...)
}

func (m *migrate) Run(ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// 1.进行 handlers 排序及 index 校验
	err := m.initHandlers()
	if err != nil {
		return err
	}
	// 2.创建 schema 表
	_, err = m.db.Exec(fmt.Sprintf(createSchemaTableQuery, m.schemaTable))
	if err != nil {
		return errors.WithStack(err)
	}
	// 3.获取当前 schema 并校验
	schema, err := m.initAndGetSchema()
	if err != nil {
		return err
	}
	if schema.version > len(m.handlers) {
		return ErrIndexLessDatabaseVersion
	}
	// 4.顺序执行
	for idx := schema.version; idx < len(m.handlers); idx++ {
		err = m.handlers[idx].Exec(ctx)
		if err != nil {
			// 发生错误时，记录 dirty 到 schema 表
			_, innerErr := m.db.Exec(fmt.Sprintf(updateDirtyQuery, m.schemaTable),
				m.handlers[idx].GetIndex(), 1)
			if innerErr != nil {
				return errors.WithStack(innerErr)
			}
			return err
		}
		// 成功时更新 version 字段
		_, err = m.db.Exec(fmt.Sprintf(updateSchemaQuery, m.schemaTable),
			m.handlers[idx].GetIndex())
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// initHandlers 初始化处理程序列表，并进行索引详细判断
func (m *migrate) initHandlers() error {
	// 1.获取所有的 handlers
	for _, e := range m.executors {
		handlers, err := e.ListHandlers()
		if err != nil {
			return err
		}
		m.handlers = append(m.handlers, handlers...)
	}
	// 2.排序
	sort.Slice(m.handlers, func(i, j int) bool {
		return m.handlers[i].GetIndex() < m.handlers[j].GetIndex()
	})
	// 3.进行 index 校验
	length := len(m.handlers)
	for i := 0; i < length-1; i++ {
		result := m.handlers[i+1].GetIndex() - m.handlers[i].GetIndex()
		if result == 1 {
			continue
		} else if result == 0 {
			return errors.Errorf(ErrDuplicateIndexFormat, m.handlers[i].GetIndex())
		} else {
			return errors.Errorf(ErrIndexGapLargeFormat, m.handlers[i].GetIndex())
		}
	}
	return nil
}

// initAndGetSchema 初始化或获取概要记录
func (m *migrate) initAndGetSchema() (*schema, error) {
	rows, err := m.db.Query(fmt.Sprintf(selectSchemaQuery, m.schemaTable))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var sche schema
	if !rows.Next() {
		_, err := m.db.Exec(fmt.Sprintf(insertDefaultSchema, m.schemaTable))
		if err != nil {
			return nil, errors.WithStack(err)
		}
	} else {
		err = rows.Scan(&sche.version, &sche.dirty)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	if sche.dirty {
		return nil, errors.Errorf(ErrFindIndexDirtyFormat, sche.version)
	}
	return &sche, nil
}

type schema struct {
	version int
	dirty   bool
}

type Option func(m *migrate)

func WithTableName(tableName string) Option {
	return func(m *migrate) {
		m.schemaTable = tableName
	}
}

func WithExecutors(executors ...Executor) Option {
	return func(m *migrate) {
		m.AddExecutors(executors...)
	}
}
