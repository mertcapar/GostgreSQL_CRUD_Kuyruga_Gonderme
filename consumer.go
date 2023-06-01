package main

import (
	"log"

	"github.com/streadway/amqp"
)

func main() {
	//RabbitMQ Sunucumuza bağlanıyoruz
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	defer conn.Close()

	//İletişim kurabilmek için kanal oluşturalım
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln(err)
	}
	defer ch.Close()

	//Kuyruğumuzu tanımlıyoruz
	_, err = ch.QueueDeclare(
		"kitap_kuyrugu", // name
		false,           // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		log.Fatalln(err)
	}

	//İşte burada kuyruğumuzu dinliyoruz.
	msgs, err := ch.Consume(
		"kitap_kuyrugu", // Bu sefer dinleyeceğim kuyruk ismini kendim yazdım
		"",              // consumer
		true,            // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		log.Fatalln(err)
	}
	//Burada goroutine ile çalışan fonksiyonumuz
	//çalışırken programın kapanmaması için
	//kanal oluşturduk
	forever := make(chan bool)

	go func() {
		//Burada eğer varsa kuyruktaki mesajları çekiyoruz
		for d := range msgs {
			//d değişkeni ile kuyruktaki mesajın bilgilerine ulaşabiliriz.
			log.Printf("Alınan mesaj: %s", d.Body)
			//Kuyruktaki mesaj ekrana bastırdık.
		}
	}()

	log.Printf(" [*] Kitap kuyruğu dinleniyor...")

	//Burada forever isimli kanalımıza değer gönderilmeyeceği için
	//programımız kapanmayacak ve sürekli olarak kuyruktaki mesajları çekecektir.
	<-forever
}
