package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "post_db"
	password = "post_db"
	dbname   = "post_db"
)

type kitap struct {
	ID     int    `json:"id"`
	Baslik string `json:"baslik"`
	Yazar  string `json:"yazar"`
	ISBN   string `json:"isbn"`
}

func CheckError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// PostgreSQL sunucusuna bağlanma
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)
	defer db.Close()

	// Eğer yoksa "kitaplar" tablosunu oluşturma
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS "kitaplar" (
	"ID" SERIAL PRIMARY KEY,
	"Baslik" TEXT,
	"Yazar" TEXT,
	"ISBN" TEXT UNIQUE
)`)
	CheckError(err)

	err = ekle(db, "Doğu Ekspresinde Cinayet", "Agatha Christie", "978975085235315")
	if err != nil {
		fmt.Println("Kitap eklenirken hata oluştu: ", err)
	} else {
		fmt.Println("Kitap başarıyla eklendi.")
	}

	/*
		err = guncelle(db, 1, "Behzat Ç", "Emrah Serbes", "97897150800759856")
		if err != nil {
			fmt.Println("Kitap güncellenirken hata oluştu: ", err)
		} else {
			fmt.Println("Kitap başarıyla güncellendi.")
		}
	*/
	// "kitaplar" tablosundaki tüm satırları seç
	rows, err := db.Query("SELECT * FROM kitaplar")
	CheckError(err)
	defer rows.Close()

	// Kitap struct'larını saklamak için bir slice oluştur
	kitaplar := []kitap{}

	// Satırlar üzerinde döngü oluştur ve her satır için bir kitap struct'ı oluştur
	for rows.Next() {
		var k kitap
		err := rows.Scan(&k.ID, &k.Baslik, &k.Yazar, &k.ISBN)
		CheckError(err)

		// Kitap struct'ını slice'a ekle
		kitaplar = append(kitaplar, k)
	}

	// Sliceyi JSON'a dönüştür
	kitaplarJson, err := json.Marshal(kitaplar)
	CheckError(err)

	// JSON'u bir dosyaya yaz
	err = ioutil.WriteFile("kitaplar.json", kitaplarJson, 0644)
	CheckError(err)

	// RabbitMQ sunucusuna bağlan
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	CheckError(err)
	defer conn.Close()

	// Bir kanal oluştur
	ch, err := conn.Channel()
	CheckError(err)
	defer ch.Close()

	// Kuyruğu bildir
	q, err := ch.QueueDeclare(
		"kitap_kuyrugu",
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	CheckError(err)

	// "kitap_kuyrugu" JSON dosyasını kuyrukta yayınla
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        kitaplarJson,
		},
	)
	CheckError(err)

}

func ekle(db *sql.DB, baslik, yazar, isbn string) error {
	_, err := db.Exec(`INSERT INTO "kitaplar" ("Baslik", "Yazar", "ISBN") VALUES ($1, $2, $3)`, baslik, yazar, isbn)
	return err
}

func guncelle(db *sql.DB, id int, baslik, yazar, isbn string) error {
	_, err := db.Exec(`UPDATE "kitaplar" SET "Baslik"=$2, "Yazar"=$3, "ISBN"=$4 WHERE "ID"=$1`, id, baslik, yazar, isbn)
	return err
}

func listele(db *sql.DB) ([]kitap, error) {
	rows, err := db.Query(`SELECT "ID", "Baslik", "Yazar", "ISBN" FROM "kitaplar"`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var kitaplar []kitap
	for rows.Next() {
		var k kitap
		err := rows.Scan(&k.ID, &k.Baslik, &k.Yazar, &k.ISBN)
		if err != nil {
			return nil, err
		}
		kitaplar = append(kitaplar, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return kitaplar, nil
}

func ara(db *sql.DB, id int) error {
	_, err := db.Query(`SELECT * FROM "kitaplar" WHERE "ID" = $1, id`)
	return err
}

func sil(db *sql.DB, id int) error {
	_, err := db.Exec(`DELETE FROM "kitaplar" WHERE "ID"=$1, id`)
	return err
}
