package main

import (
	"WB/backend/db"
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
	"strconv"
	"sync"
)

type DbHandler struct {
	DB *sql.DB
	Tx *sql.Tx
}

func Block() {
	w := sync.WaitGroup{}
	w.Add(1)
	w.Wait()
}

func JsonToOrder(content []byte) (db.Order, error) {

	order := db.Order{}
	if err := order.UnmarshalJSON(content); err != nil {
		return db.Order{}, err
	}
	return order, nil
}

func (db *DbHandler) CreateItems(ctx context.Context, order *db.Order) ([]int, error) {
	q := fmt.Sprintf("INSERT INTO items (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES",
		"chrt_id", "track_number", "price", "rid", "name", "sale", "size", "total_price", "nm_id",
		"brand", "status")
	vals := make([]any, 0)
	for i, item := range order.Items {
		if i != len(order.Items)-1 {
			q += fmt.Sprintf(" ($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d),",
				11*i+1, 11*i+2, 11*i+3, 11*i+4, 11*i+5, 11*i+6, 11*i+7, 11*i+8, 11*i+9, 11*i+10, 11*i+11)
		} else {
			q += fmt.Sprintf(" ($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d) RETURNING id;",
				11*i+1, 11*i+2, 11*i+3, 11*i+4, 11*i+5, 11*i+6, 11*i+7, 11*i+8, 11*i+9, 11*i+10, 11*i+11)
		}
		vals = append(vals, item.ChrtId)
		vals = append(vals, item.TrackNumber)
		vals = append(vals, item.Price)
		vals = append(vals, item.Rid)
		vals = append(vals, item.Name)
		vals = append(vals, item.Sale)
		vals = append(vals, item.Size)
		vals = append(vals, item.TotalPrice)
		vals = append(vals, item.NmId)
		vals = append(vals, item.Brand)
		vals = append(vals, item.Status)
	}

	result := 0
	err := db.Tx.QueryRowContext(ctx, q, vals...).Scan(&result)
	if err != nil {
		db.Tx.Rollback()
		return nil, err
	}
	ret := make([]int, 0)
	for i := result; i < result+len(order.Items); i++ {
		ret = append(ret, i)
	}
	return ret, nil
}

func (db *DbHandler) CreatePayment(ctx context.Context, order *db.Order) (int, error) {
	q := fmt.Sprintf("INSERT INTO payment (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES "+
		"($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id;",
		"transaction", "request_id", "currency", "provider", "amount", "payment_dt", "bank", "delivery_cost",
		"goods_total", "custom_fee")
	result := 0
	err := db.Tx.QueryRowContext(ctx, q, order.Payment.Transaction,
		order.Payment.RequestId, order.Payment.Currency,
		order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDt,
		order.Payment.Bank,
		order.Payment.DeliveryCost, order.Payment.GoodsTotal, order.Payment.CustomFee).Scan(&result)
	if err != nil {
		fmt.Println(err.Error())
		db.Tx.Rollback()
		return 0, nil
	}
	return result, nil
}

func (db *DbHandler) CreateDelivery(ctx context.Context, order *db.Order) (int, error) {
	q := fmt.Sprintf("INSERT INTO delivery (%s, %s, %s, %s, %s, %s, %s) VALUES "+
		"($1, $2, $3, $4, $5, $6, $7) RETURNING id;",
		"name", "phone", "zip", "city", "address", "region", "email")
	result := 0
	err := db.Tx.QueryRowContext(ctx, q,
		order.Delivery.Name,
		order.Delivery.Phone,
		order.Delivery.Zip,
		order.Delivery.City,
		order.Delivery.Address,
		order.Delivery.Region,
		order.Delivery.Email).Scan(&result)
	if err != nil {
		fmt.Println(err.Error())
		db.Tx.Rollback()
		return 0, nil
	}
	return result, nil
}

func (db *DbHandler) CreateOrder(ctx context.Context, data *db.Order) (int, error) {

	itemsVec, err := db.CreateItems(ctx, data)
	if err != nil {
		return 0, err
	}
	paymentId, err := db.CreatePayment(ctx, data)
	if err != nil {
		return 0, err
	}
	deliveryId, err := db.CreateDelivery(ctx, data)
	if err != nil {
		return 0, err
	}

	q := fmt.Sprintf("INSERT INTO orders "+
		"(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES "+
		"($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING id;",
		"order_uid", "track_number", "entry", "delivery_id", "payment_id", "items_id", "locale",
		"internal_signature", "customer_id", "delivery_service", "shardkey", "sm_id", "date_created",
		"oof_shard")

	result := 0
	err = db.Tx.QueryRowContext(ctx, q,
		data.OrderUid,
		data.TrackNumber,
		data.Entry,
		deliveryId,
		paymentId,
		pq.Array(itemsVec),
		data.Locale,
		data.InternalSignature,
		data.CustomerId,
		data.DeliveryService,
		data.Shardkey,
		data.SmId,
		data.DateCreated,
		data.OofShard).Scan(&result)
	if err != nil {
		fmt.Println(err.Error())
		db.Tx.Rollback()
		return 0, nil
	}

	return result, nil
}

func main() {
	sc, err := stan.Connect("test-cluster", "sub")
	if err != nil {
		fmt.Println("Nats-streaming not started")
		return
	}
	defer func(sc stan.Conn) {
		err := sc.Close()
		if err != nil {
			fmt.Println("Can't connect to nats-streaming")
		}
	}(sc)

	connStr := "user=wb password=sxr dbname=l0 sslmode=disable"
	dataBase, err := sql.Open("postgres", connStr)
	err = dataBase.Ping()
	if err != nil {
		fmt.Println(err.Error())
	}
	defer dataBase.Close()

	sc.Subscribe("order", func(m *stan.Msg) {
		isCorrect := true
		data, err := JsonToOrder(m.Data)
		if err != nil {
			fmt.Println("Model is incorrect")
			return
		}
		ctx := context.Background()
		tx, err := dataBase.BeginTx(ctx, nil)
		if err != nil {
			fmt.Println(err.Error())
		}
		dbH := &DbHandler{dataBase, tx}
		orderId, err := dbH.CreateOrder(ctx, &data)
		if err != nil {
			isCorrect = false
		}
		err = tx.Commit()
		if err != nil {
			isCorrect = false
		}
		if !isCorrect {
			fmt.Println("Model is incorrect")
		} else {
			fmt.Println("Order " + strconv.Itoa(orderId) + " added")
		}

	})

	Block()

}
