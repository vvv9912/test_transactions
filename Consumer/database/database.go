package database

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

type T_DB struct {
	IdPrimeryKey int
	IdUser       int     `json:"id_user"`
	Balance      float64 `json:"balance"`
}

type SettingDatabase struct {
	database   *sql.DB
	Table_name string
}

func NewDB() dbaser {
	return &SettingDatabase{}
}

type dbaser interface {
	Open() (SettingDatabase, error)
}

func Create(table_name string) error {
	connStr := "user=postgres password=postgres dbname=postgres sslmode=disable"
	driverName := "postgres"
	database, err := sql.Open(driverName, connStr)
	if err != nil {
		return err
	}
	var query string
	var variable string

	variable = "(id serial PRIMARY KEY, id_user integer,  balance FLOAT)"
	query = "CREATE TABLE IF NOT EXISTS " + table_name + " " + variable + ""
	statement, err := database.Prepare(query)
	if err != nil {
		return err
	}
	res, err := statement.Exec()
	_ = res
	defer database.Close()
	return nil
}

func (set *SettingDatabase) Open() (SettingDatabase, error) {
	// driverName := set.driverName
	// dataSourceName := set.dataSourceName
	connStr := "user=postgres password=postgres dbname=postgres sslmode=disable"
	driverName := "postgres"
	database, err := sql.Open(driverName, connStr)
	set.database = database
	return *set, err

}
func (set SettingDatabase) Close() {
	defer set.database.Close()
}
func (set SettingDatabase) Add(db T_DB) error {
	query := "INSERT INTO " + set.Table_name + " (id_user,balance) VALUES ($1,$2)"
	fmt.Println(query)
	statement, err := set.database.Prepare(query) //Подгтовленный запрос.
	if err != nil {
		fmt.Printf(err.Error())
	}

	statement.Exec(db.IdUser, db.Balance) //to do
	defer statement.Close()
	return err

}
func (set SettingDatabase) Edit(id int, change_dirr string, a interface{}) error {
	statement, err := set.database.Prepare("update " + set.Table_name + " set " + change_dirr + "=$1 where Article=$2") //Подгтовленный запрос.
	statement.Exec(a, id)
	defer statement.Close()
	return err
}
func (set SettingDatabase) Read(id_user int, db *T_DB) error {
	query := "SELECT * FROM " + set.Table_name + " where id_user = $1"
	err := set.database.QueryRow(query, id_user).Scan(&db.IdPrimeryKey, &db.IdUser, &db.Balance)
	if err != nil {
		return err
	}
	return nil
}
