package pgxdb

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

func TriggerNotify(db sqlx.Ext, event string) error {
	_, err := db.Exec("select pg_notify('events', $1)", event)
	if err != nil {
		return fmt.Errorf("Error sending notification: %v", err)
	}
	return nil
}
