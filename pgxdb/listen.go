package pgxdb

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

type ListenProcessor interface {
	Process(string, string) error
}

func ListenDBEvents(ctx context.Context, conn *pgxpool.Conn, processor ListenProcessor) error {
	_, err := conn.Exec(context.Background(), "listen events")
	if err != nil {
		return fmt.Errorf("Error listening to events channel: %w", err)
	}

	fmt.Println("Starting to listen events!")
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("Error waiting for notification: %w", err)
		}
		fmt.Println("PID:", notification.PID, "Channel:", notification.Channel, "Payload:", notification.Payload)

		// process notification
		err = processor.Process(notification.Channel, notification.Payload)
		if err != nil {
			fmt.Printf("Error while processing event! Error: %v", err)
		}
	}
}
