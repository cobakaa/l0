package main

import (
	"WB/backend/db"
	"container/list"
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
	"html/template"
	"net/http"
	"strconv"
	"sync"
)

type DBCache struct {
	Map   map[int]db.Order
	size  int
	cap   int
	Queue *list.List
	mutex *sync.RWMutex
}

func (dbh *DbHandler) NewCache() error {
	ret := &DBCache{}
	ret.Map = make(map[int]db.Order, 0)
	ret.cap = 8
	ret.size = 0
	ret.Queue = list.New()
	ret.mutex = &sync.RWMutex{}

	r, _ := dbh.DB.Query("select count(id) from orders;")
	r.Next()
	r.Scan(&dbh.DbSize)

	q := "select o.id, o.order_uid, o.track_number, o.entry, d.\"name\", d.phone, d.zip, d.city, d.address, d.region, " +
		"d.email, p.\"transaction\"," +
		"p.request_id, p.currency, p.provider, p.amount, p.payment_dt, p.bank, p.delivery_cost, p.goods_total, " +
		"p.custom_fee, o.locale, o.internal_signature,o.customer_id, o.delivery_service, o.shardkey, o.sm_id, " +
		"o.date_created, o.oof_shard, o.items_id from orders o join delivery d on delivery_id = d.id " +
		"join payment p on payment_id = p.id order by date_created limit $1;"
	record, _ := dbh.DB.Query(q, ret.cap)
	order := db.Order{}
	id := 0
	itemNums := make([]int64, 0)

	defer record.Close()
	for record.Next() {
		err := record.Scan(&id, &order.OrderUid, &order.TrackNumber, &order.Entry, &order.Delivery.Name, &order.Delivery.Phone,
			&order.Delivery.Zip, &order.Delivery.City, &order.Delivery.Address, &order.Delivery.Region, &order.Delivery.Email,
			&order.Payment.Transaction, &order.Payment.RequestId, &order.Payment.Currency, &order.Payment.Provider,
			&order.Payment.Amount, &order.Payment.PaymentDt, &order.Payment.Bank, &order.Payment.DeliveryCost,
			&order.Payment.GoodsTotal, &order.Payment.CustomFee, &order.Locale, &order.InternalSignature, &order.CustomerId,
			&order.DeliveryService, &order.Shardkey, &order.SmId, &order.DateCreated, &order.OofShard, pq.Array(&itemNums))
		if err != nil {
			return err
		}

		q = "select i.chrt_id, i.track_number , i.price ," +
			" i.rid , i.\"name\" , i.sale , i.\"size\" , i.total_price ," +
			" i.nm_id , i.brand, i.status from items i " +
			"where"
		for n, i := range itemNums {
			q += fmt.Sprintf(" i.id = %d", i)
			if n == len(itemNums)-1 {
				q += ";"
			} else {
				q += " or"
			}
		}
		record, err = dbh.DB.Query(q)
		if err != nil {
			return err
		}
		for record.Next() {
			item := db.Item{}
			err = record.Scan(&item.ChrtId, &item.TrackNumber, &item.Price, &item.Rid, &item.Name, &item.Sale,
				&item.Size, &item.TotalPrice, &item.NmId, &item.Brand, &item.Status)
			if err != nil {
				return err
			}
			order.Items = append(order.Items, item)
		}
		record.Close()

		ret.Map[id] = order
		ret.size++
		ret.Queue.PushBack(id)
	}
	dbh.Cache = ret
	fmt.Println("Cache created")
	return nil
}

type DbHandler struct {
	DB     *sql.DB
	Tx     *sql.Tx
	Cache  *DBCache
	DbSize int
}

type orderTpl struct {
	Order  *db.Order
	Id     int
	Err    error
	DbSize int
}

var funcMap = template.FuncMap{
	"inc": func(i int) int {
		return i + 1
	},
}

func JsonToOrder(content []byte) (db.Order, error) {

	order := db.Order{}
	if err := order.UnmarshalJSON(content); err != nil {
		return db.Order{}, err
	}
	return order, nil
}

func (dbh *DbHandler) CreateItems(ctx context.Context, order *db.Order) ([]int, error) {
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
	err := dbh.Tx.QueryRowContext(ctx, q, vals...).Scan(&result)
	if err != nil {
		dbh.Tx.Rollback()
		return nil, err
	}
	ret := make([]int, 0)
	for i := result; i < result+len(order.Items); i++ {
		ret = append(ret, i)
	}
	return ret, nil
}

func (dbh *DbHandler) CreatePayment(ctx context.Context, order *db.Order) (int, error) {
	q := fmt.Sprintf("INSERT INTO payment (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES "+
		"($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id;",
		"transaction", "request_id", "currency", "provider", "amount", "payment_dt", "bank", "delivery_cost",
		"goods_total", "custom_fee")
	result := 0
	err := dbh.Tx.QueryRowContext(ctx, q, order.Payment.Transaction,
		order.Payment.RequestId, order.Payment.Currency,
		order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDt,
		order.Payment.Bank,
		order.Payment.DeliveryCost, order.Payment.GoodsTotal, order.Payment.CustomFee).Scan(&result)
	if err != nil {
		fmt.Println(err.Error())
		dbh.Tx.Rollback()
		return 0, nil
	}
	return result, nil
}

func (dbh *DbHandler) CreateDelivery(ctx context.Context, order *db.Order) (int, error) {
	q := fmt.Sprintf("INSERT INTO delivery (%s, %s, %s, %s, %s, %s, %s) VALUES "+
		"($1, $2, $3, $4, $5, $6, $7) RETURNING id;",
		"name", "phone", "zip", "city", "address", "region", "email")
	result := 0
	err := dbh.Tx.QueryRowContext(ctx, q,
		order.Delivery.Name,
		order.Delivery.Phone,
		order.Delivery.Zip,
		order.Delivery.City,
		order.Delivery.Address,
		order.Delivery.Region,
		order.Delivery.Email).Scan(&result)
	if err != nil {
		fmt.Println(err.Error())
		dbh.Tx.Rollback()
		return 0, nil
	}
	return result, nil
}

func (dbh *DbHandler) CacheInsert(id int, order *db.Order) {
	dbh.Cache.mutex.Lock()
	if dbh.Cache.size+1 > dbh.Cache.cap {
		delete(dbh.Cache.Map, dbh.Cache.Queue.Front().Value.(int))
		dbh.Cache.Queue.Remove(dbh.Cache.Queue.Front())
		dbh.Cache.Queue.PushBack(id)
		dbh.Cache.size--
	}
	dbh.Cache.Map[id] = *order
	dbh.Cache.size++
	dbh.Cache.mutex.Unlock()
}

func (dbh *DbHandler) CreateOrder(ctx context.Context, data *db.Order) (int, error) {

	itemsVec, err := dbh.CreateItems(ctx, data)
	if err != nil {
		return 0, err
	}
	paymentId, err := dbh.CreatePayment(ctx, data)
	if err != nil {
		return 0, err
	}
	deliveryId, err := dbh.CreateDelivery(ctx, data)
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
	err = dbh.Tx.QueryRowContext(ctx, q,
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
		dbh.Tx.Rollback()
		return 0, nil
	}

	dbh.CacheInsert(result, data)
	dbh.DbSize++

	return result, nil
}

func (dbh *DbHandler) OrderById(id int) (db.Order, error) {
	if val, ok := dbh.Cache.Map[id]; ok {
		return val, nil
	}

	q := "select o.order_uid, o.track_number, o.entry, d.\"name\", " +
		"d.phone, d.zip, d.city, d.address, d.region, d.email, p.\"transaction\"," +
		" p.request_id , p.currency , p.provider , p.amount , p.payment_dt , p.bank , " +
		"p.delivery_cost , p.goods_total , p.custom_fee , o.locale , o.internal_signature , " +
		"o.customer_id , o.delivery_service , o.shardkey , o.sm_id , o.date_created , " +
		"o.oof_shard, o.items_id from orders o join delivery d on delivery_id = d.id join payment p on payment_id = p.id " +
		"where o.id = $1;"
	record, _ := dbh.DB.Query(q, id)
	order := db.Order{}
	itemNums := make([]int64, 0)
	record.Next()
	err := record.Scan(&order.OrderUid, &order.TrackNumber, &order.Entry, &order.Delivery.Name, &order.Delivery.Phone,
		&order.Delivery.Zip, &order.Delivery.City, &order.Delivery.Address, &order.Delivery.Region, &order.Delivery.Email,
		&order.Payment.Transaction, &order.Payment.RequestId, &order.Payment.Currency, &order.Payment.Provider,
		&order.Payment.Amount, &order.Payment.PaymentDt, &order.Payment.Bank, &order.Payment.DeliveryCost,
		&order.Payment.GoodsTotal, &order.Payment.CustomFee, &order.Locale, &order.InternalSignature, &order.CustomerId,
		&order.DeliveryService, &order.Shardkey, &order.SmId, &order.DateCreated, &order.OofShard, pq.Array(&itemNums))
	record.Close()
	if err != nil {
		return db.Order{}, err
	}

	q = "select i.chrt_id, i.track_number , i.price ," +
		" i.rid , i.\"name\" , i.sale , i.\"size\" , i.total_price ," +
		" i.nm_id , i.brand, i.status from items i " +
		"where"
	for n, i := range itemNums {
		q += fmt.Sprintf(" i.id = %d", i)
		if n == len(itemNums)-1 {
			q += ";"
		} else {
			q += " or"
		}
	}
	record, err = dbh.DB.Query(q)
	if err != nil {
		return db.Order{}, err
	}
	defer record.Close()
	for record.Next() {
		item := db.Item{}
		err = record.Scan(&item.ChrtId, &item.TrackNumber, &item.Price, &item.Rid, &item.Name, &item.Sale,
			&item.Size, &item.TotalPrice, &item.NmId, &item.Brand, &item.Status)
		if err != nil {
			return db.Order{}, err
		}
		order.Items = append(order.Items, item)
	}

	dbh.CacheInsert(id, &order)

	return order, nil
}

func (dbh *DbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		r.ParseForm()
		i, _ := strconv.Atoi(r.Form["orderId"][0])
		order, err := dbh.OrderById(i)
		var tpl = template.Must(
			template.New("index.html").Funcs(funcMap).ParseFiles("frontend/index.html"),
		)
		if err != nil {
			err = tpl.Execute(w, orderTpl{nil, i, err, dbh.DbSize})
		} else {
			err = tpl.Execute(w, orderTpl{&order, i, nil, dbh.DbSize})
		}

		if err != nil {
			fmt.Println(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else if r.Method == http.MethodGet {
		var tpl = template.Must(
			template.New("index.html").Funcs(funcMap).ParseFiles("frontend/index.html"),
		)
		err := tpl.Execute(w, orderTpl{nil, 0, nil, dbh.DbSize})
		if err != nil {
			fmt.Println(err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad request"))
	}

}

func main() {
	sc, err := stan.Connect("test-cluster", "sub")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Nats-streaming connected")
	defer func(sc stan.Conn) {
		err := sc.Close()
		if err != nil {
			fmt.Println(err.Error())
		}
	}(sc)

	connStr := "user=wb password=sxr dbname=l0 sslmode=disable"
	dataBase, err := sql.Open("postgres", connStr)
	err = dataBase.Ping()
	if err != nil {
		fmt.Println(err.Error())
	}

	handler := &DbHandler{dataBase, nil, nil, 0}
	err = handler.NewCache()
	if err != nil {
		fmt.Println(err.Error())
	}

	sc.Subscribe("order", func(m *stan.Msg) {
		isCorrect := true
		data, err := JsonToOrder(m.Data)
		//m.Ack()
		if err != nil {
			fmt.Println("Model is incorrect")
			return
		}
		ctx := context.Background()
		tx, err := dataBase.BeginTx(ctx, nil)
		if err != nil {
			fmt.Println(err.Error())
		}
		handler.Tx = tx
		orderId, err := handler.CreateOrder(ctx, &data)
		if err != nil {
			isCorrect = false
		}
		err = tx.Commit()
		if err != nil {
			isCorrect = false
		}
		if !isCorrect {
			fmt.Println("Model is incorrect")
			return
		}
		fmt.Println("Order " + strconv.Itoa(orderId) + " added")
	}, stan.DurableName("order"))

	http.Handle("/", handler)
	fmt.Println("starting server at :8080")
	err = http.ListenAndServe(":8080", handler)
	if err != nil {
		fmt.Println(err.Error())
	}

}
