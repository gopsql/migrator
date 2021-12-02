package migrator

import (
	"errors"
	"fmt"
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

	Migrations []Migration

	Migration struct {
		Version int
		Name    string
		Up      string
		Down    string
	}

	PsqlModel interface {
		Columns() []string
		ColumnDataTypes() map[string]string
		TableName() string
		Schema() string
		DropSchema() string
	}

	migration struct {
		version int
		up      string
		down    string
	}
)

func NewMigrator(optionalScope ...string) *Migrator {
	var scope string
	if len(optionalScope) > 0 {
		scope = optionalScope[0]
	}
	return &Migrator{
		Scope: scope,
	}
}

func (m *Migrator) SetConnection(dbConn db.DB) {
	m.DB = dbConn
}

func (m *Migrator) SetLogger(logger logger.Logger) {
	m.Logger = logger
}

// NewMigration generates migration file based on the differences between
// schemas in the database and psql models.
func (m *Migrator) NewMigration(models ...PsqlModel) (Migrations, error) {
	tableNames, err := m.getTableNames()
	if err != nil {
		return nil, err
	}

	columns, err := m.getColumns()
	if err != nil {
		return nil, err
	}

	var tablesCreated []string
	var columnsCreated []string
	var tablesDropped []string
	var columnsDropped []string
	var name, up, down string

	// find new tables
	for _, model := range models {
		tableName := model.TableName()
		if tableNames.has(tableName) {
			continue
		}
		up += "\n" + model.Schema()
		down = "\n" + model.DropSchema() + down
		tablesCreated = append(tablesCreated, tableName)
	}

	// find new columns
	for _, model := range models {
		tableName := model.TableName()
		if !tableNames.has(tableName) {
			continue
		}
		modelColumns := model.Columns()
		modelDataTypes := model.ColumnDataTypes()
		for _, modelColumn := range modelColumns {
			if columns.has(tableName, modelColumn) {
				continue
			}
			up += fmt.Sprintf("\nALTER TABLE %s ADD COLUMN %s %s;\n",
				tableName, modelColumn, modelDataTypes[modelColumn])
			down = fmt.Sprintf("\nALTER TABLE %s DROP COLUMN %s;\n",
				tableName, modelColumn) + down
			columnsCreated = append(columnsCreated, modelColumn+"_to_"+tableName)
		}
	}

	// remove old tables
	for _, tableName := range tableNames {
		var model PsqlModel
		for _, m := range models {
			if m.TableName() == tableName {
				model = m
				break
			}
		}
		if model != nil {
			continue
		}
		up += "\n" + "DROP TABLE IF EXISTS " + tableName + ";\n"
		down = "\n" + columns.forTable(tableName).createTable(tableName) + down
		tablesDropped = append(tablesDropped, tableName)
	}

	// remove old columns
	groups := columns.grouped()
	for tableName, cols := range groups {
		var model PsqlModel
		for _, m := range models {
			if m.TableName() == tableName {
				model = m
				break
			}
		}
		if model == nil {
			continue
		}
		modelColumns := model.Columns()
		for _, col := range cols {
			if stringSliceContains(modelColumns, col.ColumnName) {
				continue
			}
			up += "\n" + "ALTER TABLE " + tableName + " DROP COLUMN " + col.ColumnName + ";\n"
			down = "\n" + "ALTER TABLE " + tableName + " ADD COLUMN " + col.ColumnName + " " + col.dataType() + ";\n" + down
			columnsDropped = append(columnsDropped, col.ColumnName+"_from_"+tableName)
		}
	}

	if len(tablesCreated) > 0 {
		name = "create_" + strings.Join(tablesCreated, "_")
	} else if len(tablesDropped) > 0 {
		name = "drop_" + strings.Join(tablesDropped, "_")
	} else if len(columnsCreated) > 0 {
		name = "add_" + strings.Join(columnsCreated, "_")
	} else if len(columnsDropped) > 0 {
		name = "remove_" + strings.Join(columnsDropped, "_")
	}
	if name == "" {
		name = "new_migration"
	}

	var migrations Migrations
	var maxVer int
	for _, m := range m.migrations {
		if m.version > maxVer {
			maxVer = m.version
		}
	}
	if maxVer == 0 {
		migrations = append(migrations, Migration{
			Version: 0,
			Name:    "migrations",
		})
	}
	migrations = append(migrations, Migration{
		Version: maxVer + 1,
		Name:    name,
		Up:      up,
		Down:    down,
	})

	return migrations, nil
}

func (m Migration) String() string {
	if m.Version < 1 {
		return migrationsTemplate
	}
	return fmt.Sprintf(migrationTemplate, m.Version, m.Up, m.Down)
}

func (m Migration) FileName() string {
	if m.Version < 1 {
		return fmt.Sprintf("%s.go", m.Name)
	}
	return fmt.Sprintf("%02d_%s.go", m.Version, m.Name)
}

// CanonicalScope returns valid scope name.
func (m *Migrator) CanonicalScope() string {
	return strings.Join(regexp.MustCompile("[A-Za-z0-9_-]+").FindStringSubmatch(m.Scope), "")
}

// SetMigrations imports migrations.
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

// Versions returns version numbers of migrations that have been run and not
// yet been run.
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

// Migrate executes the up SQL for all the migrations that have not yet been
// run.
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

// Rollback executes the down SQL of last migration.
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

func (m *Migrator) getTableNames() (tableNames, error) {
	rows, err := m.DB.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names tableNames
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name == "schema_migrations" {
			continue
		}
		names = append(names, name)
	}
	return names, nil
}

func (m *Migrator) getColumns() (columns, error) {
	rows, err := m.DB.Query("SELECT table_name, column_name, data_type, column_default, is_nullable " +
		"FROM information_schema.columns WHERE table_schema = 'public' " +
		"ORDER BY table_name, ordinal_position")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols columns
	for rows.Next() {
		var col column
		if err := rows.Scan(
			&col.TableName, &col.ColumnName,
			&col.DataType, &col.ColumnDefault,
			&col.IsNullable,
		); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}

type (
	tableNames []string

	columns []column

	column struct {
		TableName     string
		ColumnName    string
		DataType      string
		ColumnDefault *string
		IsNullable    string
	}
)

func (names tableNames) has(tableName string) bool {
	for _, name := range names {
		if name == tableName {
			return true
		}
	}
	return false
}

func (columns columns) has(tableName, columnName string) bool {
	for _, column := range columns {
		if column.TableName == tableName && column.ColumnName == columnName {
			return true
		}
	}
	return false
}

func (columns columns) forTable(tableName string) (filtered columns) {
	for _, column := range columns {
		if column.TableName == tableName {
			filtered = append(filtered, column)
		}
	}
	return
}

func (columns columns) createTable(tableName string) string {
	if len(columns) == 0 {
		return ""
	}
	var sql []string
	for _, c := range columns {
		sql = append(sql, "\t"+c.ColumnName+" "+c.dataType())
	}
	return "CREATE TABLE " + tableName + " (\n" + strings.Join(sql, ",\n") + "\n);\n"
}

func (cs columns) grouped() (groups map[string]columns) {
	groups = map[string]columns{}
	for _, c := range cs {
		groups[c.TableName] = append(groups[c.TableName], c)
	}
	return
}

func (c column) dataType() string {
	dataType := c.DataType
	if c.ColumnDefault != nil {
		dataType += " DEFAULT " + *c.ColumnDefault
	}
	if c.IsNullable == "NO" {
		dataType += " NOT NULL"
	}
	if strings.Contains(dataType, "integer DEFAULT nextval") {
		dataType = "SERIAL PRIMARY KEY"
	}
	return dataType
}

func stringSliceContains(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}
	return false
}
