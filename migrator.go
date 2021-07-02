package migrator

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/gopsql/db"
	"github.com/gopsql/logger"
)

const (
	sqlCreateSchemaMigrations = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	scope character varying NOT NULL,
	version character varying NOT NULL
);
`

	migrationsTemplate = `package migrations

var (
	Migrations []migration
)

type (
	migration struct {
		version int
		up      string
		down    string
	}
)

func add(m migration) {
	Migrations = append(Migrations, m)
}
`

	migrationTemplate = `package migrations

func init() {
	add(migration{
		version: %d,
		up: ` + "`%s`," + `
		down: ` + "`%s`," + `
	})
}
`
)

var (
	ErrMigrationsBadType = errors.New("migrations must be slice of struct")
)

type (
	Migrator struct {
		Scope  string
		DB     db.DB
		Logger logger.Logger

		migrations []migration
	}

	migration struct {
		version int
		up      string
		down    string
	}

	psqlModel interface {
		TableName() string
		Schema() string
		DropSchema() string
	}
)

func CreateNewMigration(dir string, names ...string) (path string, err error) {
	var name string
	if len(names) > 0 {
		name = names[0]
	}
	for name == "" {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("File name in lower snake case (for example: create_table_name): ")
		name, _ = reader.ReadString('\n')
		name = strings.TrimSpace(name)
	}
	return createNewMigration(dir, name, "\n", "\n")
}

func CreateNewMigrationFromModels(dir string, models ...psqlModel) (path string, err error) {
	var name, up, down string
	for _, model := range models {
		if name == "" {
			name = "create_" + model.TableName()
		}
		up += "\n" + model.Schema()
		down = "\n" + model.DropSchema() + down
	}
	return createNewMigration(dir, name, up, down)
}

func createNewMigration(dir, name, up, down string) (path string, err error) {
	var f *os.File
	f, err = os.Open(dir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return
		}
		err = ioutil.WriteFile(filepath.Join(dir, "migrations.go"), []byte(migrationsTemplate), 0644)
		if err != nil {
			return
		}
		f, err = os.Open(dir)
	}
	if err != nil {
		return
	}
	defer f.Close()
	var fis []os.FileInfo
	fis, err = f.Readdir(-1)
	if err != nil {
		return
	}
	re := regexp.MustCompile(`^([0-9]+)(.*)\.go$`)
	max := 0
	for _, fi := range fis {
		m := re.FindStringSubmatch(fi.Name())
		if len(m) < 2 {
			continue
		}
		n, _ := strconv.Atoi(m[1])
		if n > max {
			max = n
		}
	}
	version := max
	path = filepath.Join(dir, fmt.Sprintf("%02d_%s.go", version, name))
	if _, err := os.Stat(path); err != nil {
		version = max + 1
		path = filepath.Join(dir, fmt.Sprintf("%02d_%s.go", version, name))
	}
	err = ioutil.WriteFile(path, []byte(fmt.Sprintf(migrationTemplate, version, up, down)), 0644)
	return
}

func (m *Migrator) CanonicalScope() string {
	return strings.Join(regexp.MustCompile("[A-Za-z0-9_-]+").FindStringSubmatch(m.Scope), "")
}

func (m *Migrator) SetMigrations(migrations interface{}) error {
	rt := reflect.TypeOf(migrations)
	if rt.Kind() != reflect.Slice {
		return ErrMigrationsBadType
	}
	rv := reflect.ValueOf(migrations)
	for i := 0; i < rv.Len(); i++ {
		v := rv.Index(i)
		if v.Kind() != reflect.Struct {
			return ErrMigrationsBadType
		}
		if v.FieldByName("version").Kind() != reflect.Int ||
			v.FieldByName("up").Kind() != reflect.String ||
			v.FieldByName("down").Kind() != reflect.String {
			return ErrMigrationsBadType
		}
	}
	for i := 0; i < rv.Len(); i++ {
		v := rv.Index(i)
		m.migrations = append(m.migrations, migration{
			version: int(v.FieldByName("version").Int()),
			up:      v.FieldByName("up").String(),
			down:    v.FieldByName("down").String(),
		})
	}
	return nil
}

func (m *Migrator) Versions() (migrated, unmigrated []int) {
	for _, migration := range m.migrations {
		if m.versionExists(migration.version) {
			migrated = append(migrated, migration.version)
		} else {
			unmigrated = append(unmigrated, migration.version)
		}
	}
	return
}

func (m *Migrator) Migrate() (err error) {
	if len(m.migrations) == 0 {
		m.Logger.Info("nothing to migrate")
		return
	}
	err = m.migrate()
	if err != nil {
		m.Logger.Error(err)
	}
	return
}

func (m *Migrator) migrate() error {
	_, err := m.DB.Exec(sqlCreateSchemaMigrations)
	if err != nil {
		return err
	}
	scope := m.CanonicalScope()
	migrated := false
	for _, migration := range m.migrations {
		if m.versionExists(migration.version) {
			m.Logger.Debug("version", migration.version, "already migrated")
			continue
		}
		m.Logger.Info("version", migration.version, "migrating")
		sqlStr := migration.up
		sqlStr += "\n" + fmt.Sprintf("INSERT INTO schema_migrations (scope, version) VALUES ('%s', %d);", scope, migration.version)
		m.Logger.Debug("running sql:", sqlStr)
		_, err := m.DB.Exec(sqlStr)
		if err != nil {
			return err
		}
		m.Logger.Info("version", migration.version, "migrated")
		migrated = true
	}
	if !migrated {
		m.Logger.Info("no new migrations")
	}
	return nil
}

func (m *Migrator) Rollback() (err error) {
	err = m.rollback()
	if err != nil && (err == m.DB.ErrNoRows() || m.DB.ErrGetCode(err) == "42P01") { // relation not exists
		m.Logger.Info("nothing to rollback")
	} else if err != nil {
		m.Logger.Error(err)
	}
	return
}

func (m *Migrator) rollback() error {
	version, err := m.getLatestVersion()
	if err != nil {
		return err
	}
	scope := m.CanonicalScope()
	for _, migration := range m.migrations {
		if migration.version != version {
			continue
		}
		m.Logger.Info("version", migration.version, "rollbacking")
		sqlStr := migration.down
		sqlStr += "\n" + fmt.Sprintf("DELETE FROM schema_migrations WHERE scope = '%s' AND version = '%d';", scope, version)
		m.Logger.Debug("running sql:", sqlStr)
		_, err := m.DB.Exec(sqlStr)
		if err == nil {
			m.Logger.Info("version", migration.version, "rollbacked")
		}
		return err
	}
	return nil
}

func (m *Migrator) versionExists(version int) bool {
	scope := m.CanonicalScope()
	var one int
	err := m.DB.QueryRow("SELECT 1 AS one FROM schema_migrations WHERE scope = $1 AND version = $2 LIMIT 1", scope, strconv.Itoa(version)).Scan(&one)
	if err != nil && err != m.DB.ErrNoRows() {
		m.Logger.Debug("check version:", err)
	}
	return one == 1
}

func (m *Migrator) getLatestVersion() (int, error) {
	scope := m.CanonicalScope()
	var version string
	err := m.DB.QueryRow("SELECT version FROM schema_migrations WHERE scope = $1 ORDER BY version DESC LIMIT 1", scope).Scan(&version)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(version)
}
