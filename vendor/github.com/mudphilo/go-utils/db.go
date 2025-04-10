package library

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// Db router and DB instance
type Db struct {
	DB      *sql.DB
	DBSlave *sql.DB
	TX      *sql.Tx
	Query   string
	Dialect string `default:"mysql"`
	Params  []interface{}
	Result  []interface{}
	Context context.Context
}

const DbError = "Got error  preparing a.Query %s a.Params %v error %s "

func (a *Db) StartTransaction() error {

	if a.Dialect == "postgres" {

		return fmt.Errorf("transactions are not implemented for %s", a.Dialect)

	}

	if a.Context == nil {

		tx, err := a.DB.BeginTx(a.Context, nil)
		if err != nil {

			log.Printf("error starting transaction %s ", err.Error())
			return err
		}

		a.TX = tx

	} else {

		tx, err := a.DB.Begin()
		if err != nil {

			log.Printf("error starting transaction %s ", err.Error())
			return err
		}
		a.TX = tx

	}

	return nil

}

func (a *Db) Rollback() error {

	if a.TX == nil {

		return fmt.Errorf("Transaction was not started ")

	}

	return a.TX.Rollback()

}

func (a *Db) Commit() error {

	if a.TX == nil {

		return fmt.Errorf("Transaction was not started ")

	}

	return a.TX.Commit()
}

func (a *Db) InsertQuery() (lastInsertID int64, err error) {

	if a.Dialect == "postgres" {

		var lastInsertId sql.NullInt64

		err = a.DB.QueryRow(a.Query, a.Params...).Scan(&lastInsertId)
		if err != nil {

			log.Printf(DbError, a.Query, a.Params, err.Error())
			return 0, err
		}

		return lastInsertId.Int64, nil

	}

	stmt, err := a.DB.Prepare(a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	defer stmt.Close()

	res, err := stmt.Exec(a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	return lastInsertId, nil
}

func (a *Db) InsertQueryTx() (lastInsertID int64, err error) {

	if a.TX == nil {

		if err = a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	if a.Dialect == "postgres" {

		var lastInsertId sql.NullInt64

		err = a.DB.QueryRow(a.Query, a.Params...).Scan(&lastInsertId)
		if err != nil {

			log.Printf(DbError, a.Query, a.Params, err.Error())
			return 0, err
		}

		return lastInsertId.Int64, nil

	}

	stmt, err := a.TX.Prepare(a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	defer stmt.Close()

	res, err := stmt.Exec(a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	return lastInsertId, nil
}

func (a *Db) InsertQueryWithContext() (lastInsertID int64, err error) {

	if a.Dialect == "postgres" {

		var lastInsertId sql.NullInt64

		err = a.DB.QueryRowContext(a.Context, a.Query, a.Params...).Scan(&lastInsertId)
		if err != nil {

			log.Printf(DbError, a.Query, a.Params, err.Error())
			return 0, err
		}

		return lastInsertId.Int64, nil

	}

	stmt, err := a.DB.PrepareContext(a.Context, a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	defer stmt.Close()

	res, err := stmt.ExecContext(a.Context, a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	return lastInsertId, nil
}

func (a *Db) InsertQueryWithContextTx() (lastInsertID int64, err error) {

	if a.Dialect == "postgres" {

		var lastInsertId sql.NullInt64

		err = a.DB.QueryRowContext(a.Context, a.Query, a.Params...).Scan(&lastInsertId)
		if err != nil {

			log.Printf(DbError, a.Query, a.Params, err.Error())
			return 0, err
		}

		return lastInsertId.Int64, nil

	}

	if a.TX == nil {

		if err = a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	stmt, err := a.TX.PrepareContext(a.Context, a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	defer stmt.Close()

	res, err := stmt.ExecContext(a.Context, a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	return lastInsertId, nil
}

func (a *Db) UpdateQuery() (rowsAffected int64, err error) {

	stmt, err := a.DB.Prepare(a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	defer stmt.Close()

	res, err := stmt.Exec(a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	rowsaffected, err := res.RowsAffected()
	if err != nil {
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	return rowsaffected, nil
}

func (a *Db) UpdateQueryTx() (rowsAffected int64, err error) {

	if a.TX == nil {

		if err = a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	stmt, err := a.TX.Prepare(a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	defer stmt.Close()

	res, err := stmt.Exec(a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	rowsaffected, err := res.RowsAffected()
	if err != nil {
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	return rowsaffected, nil
}

func (a *Db) UpdateQueryWithContext() (rowsAffected int64, err error) {

	stmt, err := a.DB.PrepareContext(a.Context, a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	defer stmt.Close()

	res, err := stmt.ExecContext(a.Context, a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	rowsaffected, err := res.RowsAffected()
	if err != nil {
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	return rowsaffected, nil
}

func (a *Db) UpdateQueryWithContextTx() (rowsAffected int64, err error) {

	if a.TX == nil {

		if err = a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	stmt, err := a.TX.PrepareContext(a.Context, a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	defer stmt.Close()

	res, err := stmt.ExecContext(a.Context, a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	rowsaffected, err := res.RowsAffected()
	if err != nil {
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return 0, err
	}

	return rowsaffected, nil
}

func (a *Db) InsertInTransaction() (lastInsertID *int64, err error) {

	wasNil := false

	stmt, err := a.TX.Prepare(a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.Exec(a.Params...)
	if err != nil {

		if wasNil {

			_ = a.TX.Rollback()
		}
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {

		if wasNil {

			_ = a.TX.Rollback()
		}
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	if wasNil {

		_ = a.TX.Rollback()
	}

	return &lastInsertId, nil
}

func (a *Db) InsertInTransactionWithContext() (lastInsertID *int64, err error) {

	wasNil := false

	if a.TX == nil {

		wasNil = true
		a.TX, err = a.DB.BeginTx(a.Context, nil)
		if err != nil {

			log.Printf("Got error starting transaction %s ", err.Error())
			return nil, err
		}

	}

	stmt, err := a.TX.PrepareContext(a.Context, a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.ExecContext(a.Context, a.Params...)
	if err != nil {

		if wasNil {

			_ = a.TX.Rollback()
		}
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {

		if wasNil {

			_ = a.TX.Rollback()
		}
		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	if wasNil {

		_ = a.TX.Rollback()
	}

	return &lastInsertId, nil
}

func (a *Db) InsertIgnore() (lastInsertID *int64, err error) {

	if a.Dialect == "postgres" {

	}

	stmt, err := a.DB.Prepare(a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.Exec(a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, nil
	}

	return &lastInsertId, nil
}

func (a *Db) InsertIgnoreTx() (lastInsertID *int64, err error) {

	if a.TX == nil {

		if err = a.StartTransaction(); err != nil {

			return nil, err
		}
	}

	stmt, err := a.TX.Prepare(a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.Exec(a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, nil
	}

	return &lastInsertId, nil
}

func (a *Db) InsertIgnoreWithContext() (lastInsertID *int64, err error) {

	if a.Dialect == "postgres" {

	}

	stmt, err := a.DB.PrepareContext(a.Context, a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.ExecContext(a.Context, a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, nil
	}

	return &lastInsertId, nil
}

func (a *Db) InsertIgnoreWithContextTx() (lastInsertID *int64, err error) {

	if a.TX == nil {

		if err = a.StartTransaction(); err != nil {

			return nil, err
		}
	}

	stmt, err := a.TX.PrepareContext(a.Context, a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.ExecContext(a.Context, a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, nil
	}

	return &lastInsertId, nil
}

// Deprecated: Use InsertIgnoreTx
func (a *Db) InsertIgnoreInTransaction() (lastInsertID *int64, err error) {

	stmt, err := a.TX.Prepare(a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.Exec(a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, nil
	}

	return &lastInsertId, nil
}

// Deprecated: Use InsertIgnoreWithContextTx
func (a *Db) InsertIgnoreInTransactionWithContext() (lastInsertID *int64, err error) {

	stmt, err := a.TX.PrepareContext(a.Context, a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.ExecContext(a.Context, a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	lastInsertId, err := res.LastInsertId()
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, nil
	}

	return &lastInsertId, nil
}

// Deprecated: Use UpdateQueryTx
func (a *Db) UpdateInTransaction() (rowsAffected *int64, err error) {

	stmt, err := a.TX.Prepare(a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.Exec(a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	rowsaffected, err := res.RowsAffected()
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	return &rowsaffected, nil
}

// Deprecated: Use UpdateQueryWithContextTx
func (a *Db) UpdateInTransactionWithContext() (rowsAffected *int64, err error) {

	stmt, err := a.TX.PrepareContext(a.Context, a.Query)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.ExecContext(a.Context, a.Params...)
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	rowsaffected, err := res.RowsAffected()
	if err != nil {

		log.Printf(DbError, a.Query, a.Params, err.Error())
		return nil, err
	}

	return &rowsaffected, nil
}

func (a *Db) FetchOne() *sql.Row {

	if a.Dialect == "mysql" {

		_, err := a.DB.Exec("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
		if err != nil {

			log.Printf("error disabling ONLY_FULL_GROUP_BY %s", err.Error())
		}

	}

	a.removeValidParameters()

	if a.Params == nil || len(a.Params) == 0 {

		return a.DB.QueryRow(a.Query)

	}

	return a.DB.QueryRow(a.Query, a.Params...)
}

func (a *Db) FetchOneSlave() *sql.Row {

	if a.Dialect == "mysql" {

		_, err := a.DB.Exec("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
		if err != nil {

			log.Printf("error disabling ONLY_FULL_GROUP_BY %s", err.Error())
		}

	}

	a.removeValidParameters()

	if a.Params == nil || len(a.Params) == 0 {

		return a.DBSlave.QueryRow(a.Query)

	}

	return a.DBSlave.QueryRow(a.Query, a.Params...)
}

func (a *Db) FetchOneWithContext() *sql.Row {

	if a.Context == nil {

		return a.FetchOne()
	}

	if a.Dialect == "mysql" {

		_, err := a.DB.ExecContext(a.Context, "SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
		if err != nil {

			log.Printf("error disabling ONLY_FULL_GROUP_BY %s", err.Error())
		}

	}

	a.removeValidParameters()

	if a.Params == nil || len(a.Params) == 0 {

		return a.DB.QueryRowContext(a.Context, a.Query)

	}

	return a.DB.QueryRowContext(a.Context, a.Query, a.Params...)
}

func (a *Db) FetchOneSlaveWithContext() *sql.Row {

	if a.Context == nil {

		return a.FetchOneSlave()
	}

	if a.Dialect == "mysql" {

		_, err := a.DB.ExecContext(a.Context, "SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
		if err != nil {

			log.Printf("error disabling ONLY_FULL_GROUP_BY %s", err.Error())
		}

	}

	a.removeValidParameters()

	if a.Params == nil || len(a.Params) == 0 {

		return a.DBSlave.QueryRowContext(a.Context, a.Query)

	}

	return a.DBSlave.QueryRowContext(a.Context, a.Query, a.Params...)
}

func (a *Db) Fetch() (*sql.Rows, error) {

	if a.Dialect == "mysql" {

		_, err := a.DB.Exec("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
		if err != nil {

			log.Printf("error disabling ONLY_FULL_GROUP_BY %s", err.Error())
		}

	}

	a.removeValidParameters()

	if a.Params == nil || len(a.Params) == 0 {

		rows, err := a.DB.Query(a.Query)
		if err != nil {

			log.Printf("error fetching results from database using query %s | no params |  error %s", a.Query, err.Error())
		}

		return rows, err

	}

	rows, err := a.DB.Query(a.Query, a.Params...)
	if err != nil {

		log.Printf("error fetching results from database using query %s | params %v |  error %s", a.Query, a.Params, err.Error())
	}

	return rows, err
}

func (a *Db) FetchSlave() (*sql.Rows, error) {

	if a.Dialect == "mysql" {

		_, err := a.DB.Exec("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
		if err != nil {

			log.Printf("error disabling ONLY_FULL_GROUP_BY %s", err.Error())
		}

	}

	a.removeValidParameters()

	if a.Params == nil || len(a.Params) == 0 {

		rows, err := a.DBSlave.Query(a.Query)
		if err != nil {

			log.Printf("error fetching results from database using query %s | no params |  error %s", a.Query, err.Error())
		}

		return rows, err

	}

	rows, err := a.DBSlave.Query(a.Query, a.Params...)
	if err != nil {

		log.Printf("error fetching results from database using query %s | params %v |  error %s", a.Query, a.Params, err.Error())
	}

	return rows, err
}

func (a *Db) FetchWithContext() (*sql.Rows, error) {

	if a.Context == nil {

		return a.Fetch()
	}

	if a.Dialect == "mysql" {

		_, err := a.DB.ExecContext(a.Context, "SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
		if err != nil {

			log.Printf("error disabling ONLY_FULL_GROUP_BY %s", err.Error())
		}

	}

	a.removeValidParameters()

	if a.Params == nil || len(a.Params) == 0 {

		rows, err := a.DB.QueryContext(a.Context, a.Query)
		if err != nil {

			log.Printf("error fetching results from database using query %s | no params |  error %s", a.Query, err.Error())
		}

		return rows, err

	}

	rows, err := a.DB.QueryContext(a.Context, a.Query, a.Params...)
	if err != nil {

		log.Printf("error fetching results from database using query %s | params %v |  error %s", a.Query, a.Params, err.Error())
	}

	return rows, err
}

func (a *Db) FetchSlaveWithContext() (*sql.Rows, error) {

	if a.Context == nil {

		return a.FetchSlave()
	}

	if a.Dialect == "mysql" {

		_, err := a.DB.ExecContext(a.Context, "SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
		if err != nil {

			log.Printf("error disabling ONLY_FULL_GROUP_BY %s", err.Error())
		}

	}

	a.removeValidParameters()

	if a.Params == nil || len(a.Params) == 0 {

		rows, err := a.DBSlave.QueryContext(a.Context, a.Query)
		if err != nil {

			log.Printf("error fetching results from database using query %s | no params |  error %s", a.Query, err.Error())
		}

		return rows, err

	}

	rows, err := a.DBSlave.QueryContext(a.Context, a.Query, a.Params...)
	if err != nil {

		log.Printf("error fetching results from database using query %s | params %v |  error %s", a.Query, a.Params, err.Error())
	}

	return rows, err
}

func (a *Db) SetParams(params ...interface{}) {

	a.Params = params
}

func (a *Db) SetQuery(query string) {

	a.Query = query
}

func (a *Db) setResults(result ...interface{}) {

	a.Result = result
}

func (a *Db) InsertWithContext(tableName string, data map[string]interface{}) (int64, error) {

	var placeHoldersParts, columns []string
	var params []interface{}

	x := 0

	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)
		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	sqlQueryParts := fmt.Sprintf("INSERT IGNORE INTO %s (%s) %s (%s) ", tableName, strings.Join(columns, ","), a.getValueKeyword(), strings.Join(placeHoldersParts, ","))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQueryWithContext()
}

func (a *Db) InsertWithContextTx(tableName string, data map[string]interface{}) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var placeHoldersParts, columns []string
	var params []interface{}

	x := 0

	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)
		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	sqlQueryParts := fmt.Sprintf("INSERT IGNORE INTO %s (%s) %s (%s) ", tableName, strings.Join(columns, ","), a.getValueKeyword(), strings.Join(placeHoldersParts, ","))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQueryWithContextTx()
}

func (a *Db) Insert(tableName string, data map[string]interface{}) (int64, error) {

	var placeHoldersParts, columns []string
	var params []interface{}

	x := 0

	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)
		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	sqlQueryParts := fmt.Sprintf("INSERT IGNORE INTO %s (%s) %s (%s) ", tableName, strings.Join(columns, ","), a.getValueKeyword(), strings.Join(placeHoldersParts, ","))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQuery()
}

func (a *Db) InsertTx(tableName string, data map[string]interface{}) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var placeHoldersParts, columns []string
	var params []interface{}

	x := 0

	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)
		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	sqlQueryParts := fmt.Sprintf("INSERT IGNORE INTO %s (%s) %s (%s) ", tableName, strings.Join(columns, ","), a.getValueKeyword(), strings.Join(placeHoldersParts, ","))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQueryTx()
}

func (a *Db) UpsertWithContext(tableName string, data map[string]interface{}, updates []string) (int64, error) {

	var placeHoldersParts, updatesPart, columns []string
	var params []interface{}

	x := 0

	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)
		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	updateString := ""

	if updates != nil {

		for _, f := range updates {

			updatesPart = append(updatesPart, fmt.Sprintf("%s=VALUES(%s)", f, f))
		}

		updateString = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s ", strings.Join(updatesPart, ","))
	}

	sqlQueryParts := fmt.Sprintf("INSERT INTO %s (%s) %s (%s) %s", tableName, strings.Join(columns, ","), a.getValueKeyword(), strings.Join(placeHoldersParts, ","), updateString)

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQueryWithContext()
}

func (a *Db) UpsertWithContextTx(tableName string, data map[string]interface{}, updates []string) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var placeHoldersParts, updatesPart, columns []string
	var params []interface{}

	x := 0

	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)
		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	updateString := ""

	if updates != nil {

		for _, f := range updates {

			updatesPart = append(updatesPart, fmt.Sprintf("%s=VALUES(%s)", f, f))
		}

		updateString = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s ", strings.Join(updatesPart, ","))
	}

	sqlQueryParts := fmt.Sprintf("INSERT INTO %s (%s) %s (%s) %s", tableName, strings.Join(columns, ","), a.getValueKeyword(), strings.Join(placeHoldersParts, ","), updateString)

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQueryWithContextTx()
}

func (a *Db) Upsert(tableName string, data map[string]interface{}, updates []string) (int64, error) {

	var placeHoldersParts, updatesPart, columns []string
	var params []interface{}

	x := 0

	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)
		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	updateString := ""

	if updates != nil {

		for _, f := range updates {

			updatesPart = append(updatesPart, fmt.Sprintf("%s=VALUES(%s)", f, f))
		}

		updateString = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s ", strings.Join(updatesPart, ","))
	}

	sqlQueryParts := fmt.Sprintf("INSERT INTO %s (%s) %s (%s) %s", tableName, strings.Join(columns, ","), a.getValueKeyword(), strings.Join(placeHoldersParts, ","), updateString)

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQuery()
}

func (a *Db) UpsertTx(tableName string, data map[string]interface{}, updates []string) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var placeHoldersParts, updatesPart, columns []string
	var params []interface{}

	x := 0

	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)
		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	updateString := ""

	if updates != nil {

		for _, f := range updates {

			updatesPart = append(updatesPart, fmt.Sprintf("%s=VALUES(%s)", f, f))
		}

		updateString = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s ", strings.Join(updatesPart, ","))
	}

	sqlQueryParts := fmt.Sprintf("INSERT INTO %s (%s) %s (%s) %s", tableName, strings.Join(columns, ","), a.getValueKeyword(), strings.Join(placeHoldersParts, ","), updateString)

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQueryTx()
}

func (a *Db) Update(tableName string, andCondition, data map[string]interface{}) (int64, error) {

	var conditions, columns []string
	var params []interface{}

	x := 0
	for column, param := range data {

		x++
		params = append(params, param)
		if a.Dialect == "postgres" {

			columns = append(columns, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			columns = append(columns, fmt.Sprintf("%s = ? ", column))

		}
	}

	for column, value := range andCondition {

		x++
		if a.Dialect == "postgres" {

			conditions = append(conditions, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			conditions = append(conditions, fmt.Sprintf("%s = ? ", column))

		}

		params = append(params, value)

	}

	sqlQueryParts := fmt.Sprintf("UPDATE  %s SET %s WHERE %s ", tableName, strings.Join(columns, ","), strings.Join(conditions, " AND "))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.UpdateQuery()
}

func (a *Db) UpdateTx(tableName string, andCondition, data map[string]interface{}) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var conditions, columns []string
	var params []interface{}

	x := 0
	for column, param := range data {

		x++
		params = append(params, param)
		if a.Dialect == "postgres" {

			columns = append(columns, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			columns = append(columns, fmt.Sprintf("%s = ? ", column))

		}
	}

	for column, value := range andCondition {

		x++
		if a.Dialect == "postgres" {

			conditions = append(conditions, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			conditions = append(conditions, fmt.Sprintf("%s = ? ", column))

		}

		params = append(params, value)

	}

	sqlQueryParts := fmt.Sprintf("UPDATE  %s SET %s WHERE %s ", tableName, strings.Join(columns, ","), strings.Join(conditions, " AND "))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.UpdateQueryTx()
}

func (a *Db) UpdateWithContext(tableName string, andCondition, data map[string]interface{}) (int64, error) {

	var conditions, columns []string
	var params []interface{}

	x := 0
	for column, param := range data {

		x++
		params = append(params, param)
		if a.Dialect == "postgres" {

			columns = append(columns, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			columns = append(columns, fmt.Sprintf("%s = ? ", column))

		}
	}

	for column, value := range andCondition {

		x++
		if a.Dialect == "postgres" {

			conditions = append(conditions, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			conditions = append(conditions, fmt.Sprintf("%s = ? ", column))

		}

		params = append(params, value)

	}

	sqlQueryParts := fmt.Sprintf("UPDATE  %s SET %s WHERE %s ", tableName, strings.Join(columns, ","), strings.Join(conditions, " AND "))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.UpdateQueryWithContext()
}

func (a *Db) UpdateWithContextTx(tableName string, andCondition, data map[string]interface{}) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var conditions, columns []string
	var params []interface{}

	x := 0
	for column, param := range data {

		x++
		params = append(params, param)
		if a.Dialect == "postgres" {

			columns = append(columns, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			columns = append(columns, fmt.Sprintf("%s = ? ", column))

		}
	}

	for column, value := range andCondition {

		x++
		if a.Dialect == "postgres" {

			conditions = append(conditions, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			conditions = append(conditions, fmt.Sprintf("%s = ? ", column))

		}

		params = append(params, value)

	}

	sqlQueryParts := fmt.Sprintf("UPDATE  %s SET %s WHERE %s ", tableName, strings.Join(columns, ","), strings.Join(conditions, " AND "))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.UpdateQueryWithContextTx()
}

func (a *Db) Delete(tableName string, andCondition map[string]interface{}) (int64, error) {

	var conditions []string
	var params []interface{}

	x := 0
	for column, value := range andCondition {

		x++
		if a.Dialect == "postgres" {

			conditions = append(conditions, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			conditions = append(conditions, fmt.Sprintf("%s = ? ", column))

		}
		params = append(params, value)
	}

	sqlQueryParts := fmt.Sprintf("DELETE FROM %s WHERE %s ", tableName, strings.Join(conditions, " AND "))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.UpdateQuery()
}

func (a *Db) DeleteTx(tableName string, andCondition map[string]interface{}) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var conditions []string
	var params []interface{}

	x := 0
	for column, value := range andCondition {

		x++
		if a.Dialect == "postgres" {

			conditions = append(conditions, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			conditions = append(conditions, fmt.Sprintf("%s = ? ", column))

		}
		params = append(params, value)
	}

	sqlQueryParts := fmt.Sprintf("DELETE FROM %s WHERE %s ", tableName, strings.Join(conditions, " AND "))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.UpdateQueryTx()
}

func (a *Db) DeleteWithContext(tableName string, andCondition map[string]interface{}) (int64, error) {

	var conditions []string
	var params []interface{}

	x := 0
	for column, value := range andCondition {

		x++
		if a.Dialect == "postgres" {

			conditions = append(conditions, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			conditions = append(conditions, fmt.Sprintf("%s = ? ", column))

		}
		params = append(params, value)
	}

	sqlQueryParts := fmt.Sprintf("DELETE FROM %s WHERE %s ", tableName, strings.Join(conditions, " AND "))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.UpdateQueryWithContext()
}

func (a *Db) DeleteWithContextTx(tableName string, andCondition map[string]interface{}) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var conditions []string
	var params []interface{}

	x := 0
	for column, value := range andCondition {

		x++
		if a.Dialect == "postgres" {

			conditions = append(conditions, fmt.Sprintf("%s = $%d ", column, x))

		} else {

			conditions = append(conditions, fmt.Sprintf("%s = ? ", column))

		}
		params = append(params, value)
	}

	sqlQueryParts := fmt.Sprintf("DELETE FROM %s WHERE %s ", tableName, strings.Join(conditions, " AND "))

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.UpdateQueryWithContextTx()
}

func (a *Db) UpsertData(tableName string, primaryKey string, data map[string]interface{}, conflicts, updates []string) (int64, error) {

	var placeHoldersParts, updatesPart, columns []string
	var params []interface{}

	x := 0
	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)

		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	updateString := ""

	if updates != nil {

		for _, f := range updates {

			if a.Dialect == "postgres" {

				//excluded.
				updatesPart = append(updatesPart, fmt.Sprintf("%s=excluded.%s", f, f))

			} else {

				updatesPart = append(updatesPart, fmt.Sprintf("%s=VALUES(%s)", f, f))

			}
		}

		if a.Dialect == "postgres" {

			updateString = fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s ", strings.Join(conflicts, ","), strings.Join(updatesPart, ","))

		} else {

			updateString = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s ", strings.Join(updatesPart, ","))

		}
	}

	var sqlQueryParts = ""

	if a.Dialect == "postgres" {

		if len(primaryKey) > 0 {

			sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s RETURNING %s", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString, primaryKey)

		} else {

			sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s ", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString)

		}

	} else {

		sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUE (%s) %s", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString)

	}

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQuery()
}

func (a *Db) UpsertDataTx(tableName string, primaryKey string, data map[string]interface{}, conflicts, updates []string) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var placeHoldersParts, updatesPart, columns []string
	var params []interface{}

	x := 0
	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)

		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	updateString := ""

	if updates != nil {

		for _, f := range updates {

			if a.Dialect == "postgres" {

				//excluded.
				updatesPart = append(updatesPart, fmt.Sprintf("%s=excluded.%s", f, f))

			} else {

				updatesPart = append(updatesPart, fmt.Sprintf("%s=VALUES(%s)", f, f))

			}
		}

		if a.Dialect == "postgres" {

			updateString = fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s ", strings.Join(conflicts, ","), strings.Join(updatesPart, ","))

		} else {

			updateString = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s ", strings.Join(updatesPart, ","))

		}
	}

	var sqlQueryParts = ""

	if a.Dialect == "postgres" {

		if len(primaryKey) > 0 {

			sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s RETURNING %s", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString, primaryKey)

		} else {

			sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s ", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString)

		}

	} else {

		sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUE (%s) %s", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString)

	}

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQueryTx()
}

func (a *Db) UpsertDataWithContext(tableName string, primaryKey string, data map[string]interface{}, conflicts, updates []string) (int64, error) {

	var placeHoldersParts, updatesPart, columns []string
	var params []interface{}

	x := 0
	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)

		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	updateString := ""

	if updates != nil {

		for _, f := range updates {

			if a.Dialect == "postgres" {

				//excluded.
				updatesPart = append(updatesPart, fmt.Sprintf("%s=excluded.%s", f, f))

			} else {

				updatesPart = append(updatesPart, fmt.Sprintf("%s=VALUES(%s)", f, f))

			}
		}

		if a.Dialect == "postgres" {

			updateString = fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s ", strings.Join(conflicts, ","), strings.Join(updatesPart, ","))

		} else {

			updateString = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s ", strings.Join(updatesPart, ","))

		}
	}

	var sqlQueryParts = ""

	if a.Dialect == "postgres" {

		if len(primaryKey) > 0 {

			sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s RETURNING %s", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString, primaryKey)

		} else {

			sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s ", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString)

		}

	} else {

		sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUE (%s) %s", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString)

	}

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQueryWithContext()
}

func (a *Db) UpsertDataWithContextTx(tableName string, primaryKey string, data map[string]interface{}, conflicts, updates []string) (int64, error) {

	if a.TX == nil {

		if err := a.StartTransaction(); err != nil {

			return 0, err
		}
	}

	var placeHoldersParts, updatesPart, columns []string
	var params []interface{}

	x := 0
	for column, param := range data {

		x++
		params = append(params, param)
		columns = append(columns, column)

		if a.Dialect == "postgres" {

			placeHoldersParts = append(placeHoldersParts, fmt.Sprintf("$%d", x))

		} else {

			placeHoldersParts = append(placeHoldersParts, "?")

		}
	}

	updateString := ""

	if updates != nil {

		for _, f := range updates {

			if a.Dialect == "postgres" {

				//excluded.
				updatesPart = append(updatesPart, fmt.Sprintf("%s=excluded.%s", f, f))

			} else {

				updatesPart = append(updatesPart, fmt.Sprintf("%s=VALUES(%s)", f, f))

			}
		}

		if a.Dialect == "postgres" {

			updateString = fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s ", strings.Join(conflicts, ","), strings.Join(updatesPart, ","))

		} else {

			updateString = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s ", strings.Join(updatesPart, ","))

		}
	}

	var sqlQueryParts = ""

	if a.Dialect == "postgres" {

		if len(primaryKey) > 0 {

			sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s RETURNING %s", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString, primaryKey)

		} else {

			sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s ", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString)

		}

	} else {

		sqlQueryParts = fmt.Sprintf("INSERT INTO %s (%s) VALUE (%s) %s", tableName, strings.Join(columns, ","), strings.Join(placeHoldersParts, ","), updateString)

	}

	a.SetQuery(sqlQueryParts)
	a.SetParams(params...)
	return a.InsertQueryWithContextTx()
}

func (a *Db) getValueKeyword() string {

	if a.Dialect == "postgres" {

		return "VALUES"
	}

	return "VALUES"
}

func (a *Db) removeValidParameters() {

	var par []interface{}

	for _, p := range a.Params {

		if p != nil {

			par = append(par, p)
		}
	}

	a.Params = par

}
