package concrete

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"powerlaw.ai/powerlib/migrate"
)

var (
	ErrFileType = errors.New("file type is not supported")
	ErrFileName = errors.New("file name is illegal")
)

const (
	sqlErrorFmt = "error sql is : %s"
)

const (
	defaultSourceDir = "./migration"

	sqlExt = ".sql"
)

// sqlExecutor 存储具体 db 连接，sql 处理单元，读取文件的目录
type sqlExecutor struct {
	sync.Mutex

	sourceDir string
	db        *sql.DB

	handlers []migrate.Handler
}

func NewSQLExecutor(db *sql.DB, sourceDir string) migrate.Executor {
	if sourceDir == "" {
		sourceDir = defaultSourceDir
	}
	return &sqlExecutor{
		db:        db,
		sourceDir: sourceDir,
	}
}

func (s *sqlExecutor) ListHandlers() ([]migrate.Handler, error) {
	s.Mutex.Lock()
	defer s.Unlock()
	if len(s.handlers) != 0 {
		return s.handlers, nil
	}
	err := s.initHandlers()
	return s.handlers, errors.WithStack(err)
}

// initHandlers 初始化 sql 处理程序
func (s *sqlExecutor) initHandlers() error {
	// 1.读取文件夹中的所有 .sql 文件
	files, err := getFilesByDir(s.sourceDir)
	if err != nil {
		return nil
	}
	// 2.每个文件生成一个 sqlHandler
	var handlers []migrate.Handler
	for _, f := range files {
		// 读取文件信息
		file, err := os.Open(path.Join(s.sourceDir, f.fileName))
		if err != nil {
			return errors.WithStack(err)
		}
		buf := new(bytes.Buffer)
		_, err = io.Copy(buf, file)

		content := make([]byte, buf.Len())
		_, err = buf.Read(content)
		if err != nil {
			return errors.WithStack(err)
		}
		err = file.Close()
		if err != nil {
			return errors.WithStack(err)
		}
		// 制作 sql 处理程序
		handlers = append(handlers, &sqlHandler{
			baseHandler: baseHandler{f.index},
			query:       string(content),
			db:          s.db,
		})
	}
	s.handlers = handlers
	return nil
}

type fileInfo struct {
	index    int
	fileName string
	ext      string
}

// getFilesByDir 获取目录下所有的 .sql 文件
func getFilesByDir(dir string) ([]fileInfo, error) {
	dirs, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var fileInfos []fileInfo
	for _, dir := range dirs {
		if dir.IsDir() {
			return nil, ErrFileType
		}
		fileName := dir.Name()
		ext := path.Ext(fileName)
		// 忽略所有非 .sql 文件
		if ext != sqlExt {
			continue
		}
		nameSplit := strings.Split(fileName, "_")

		num, err := strconv.ParseInt(nameSplit[0], 10, 64)
		if err != nil {
			return nil, ErrFileName
		}
		fileInfos = append(fileInfos, fileInfo{
			index:    int(num),
			fileName: fileName,
			ext:      ext,
		})
	}
	return fileInfos, nil
}

// sqlHandler 包含具体 sql 语句
type sqlHandler struct {
	baseHandler
	query string
	db    *sql.DB
}

func (s *sqlHandler) GetIndex() int {
	return s.index
}

func (s *sqlHandler) Exec(ctx context.Context) error {
	tx, err := s.db.Begin()
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = tx.Exec(s.query)
	if err != nil {
		tx.Rollback()
		return errors.WithMessagef(err, sqlErrorFmt, s.query)
	}
	tx.Commit()
	return nil
}
