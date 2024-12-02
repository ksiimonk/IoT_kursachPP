package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq"
)

// SensorData представляет данные, полученные от IoT устройства
type SensorData struct {
	DeviceID    string  `json:"device_id"`
	Timestamp   string  `json:"timestamp"`
	Temperature float64 `json:"temperature,omitempty"`
	Humidity    float64 `json:"humidity,omitempty"`
}

// Константы для подключения к PostgreSQL
const (
	PostgresHost     = "localhost"
	PostgresPort     = 5432
	PostgresUser     = "admin"
	PostgresPassword = "admin"
	PostgresDB       = "iot_db"
)

// Подключение к базе данных
func connectToDB() (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		PostgresHost, PostgresPort, PostgresUser, PostgresPassword, PostgresDB)
	return sql.Open("postgres", connStr)
}

// Запись данных в базу
func saveDataToDB(db *sql.DB, data SensorData) error {
	query := `INSERT INTO sensor_data (device_id, timestamp, temperature, humidity) VALUES ($1, $2, $3, $4)`
	_, err := db.Exec(query, data.DeviceID, data.Timestamp, data.Temperature, data.Humidity)
	return err
}

// Основная функция для обработки сообщений MQTT
func handleMessage(db *sql.DB) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Получено сообщение из топика %s: %s\n", msg.Topic(), string(msg.Payload()))

		// Парсинг сообщения
		var data SensorData
		err := json.Unmarshal(msg.Payload(), &data)
		if err != nil {
			log.Printf("Ошибка парсинга сообщения: %v", err)
			return
		}

		// Добавление временной метки, если отсутствует
		if data.Timestamp == "" {
			data.Timestamp = time.Now().Format(time.RFC3339)
		}

		// Сохранение данных в базу
		err = saveDataToDB(db, data)
		if err != nil {
			log.Printf("Ошибка сохранения данных в базу: %v", err)
		} else {
			fmt.Println("Данные успешно сохранены в базу")
		}
	}
}

func main() {
	// Подключение к PostgreSQL
	db, err := connectToDB()
	if err != nil {
		log.Fatalf("Ошибка подключения к базе данных: %v", err)
	}
	defer db.Close()

	// Проверка подключения
	err = db.Ping()
	if err != nil {
		log.Fatalf("Ошибка проверки подключения к базе данных: %v", err)
	}

	fmt.Println("Подключение к базе данных установлено")

	// Настройка подключения к MQTT брокеру
	opts := mqtt.NewClientOptions().AddBroker("tcp://localhost:1883")
	opts.SetClientID("iot-monitoring-service")
	opts.SetKeepAlive(60 * time.Second)
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Получено сообщение из неизвестного топика %s: %s\n", msg.Topic(), string(msg.Payload()))
	})
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		fmt.Printf("Соединение с брокером потеряно: %v\n", err)
	})
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		fmt.Println("Подключение к MQTT брокеру установлено")
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Ошибка подключения к MQTT брокеру: %v", token.Error())
	}

	// Подписка на топики
	topics := []string{"sensors/temperature", "sensors/humidity"}
	for _, topic := range topics {
		if token := client.Subscribe(topic, 1, handleMessage(db)); token.Wait() && token.Error() != nil {
			log.Fatalf("Ошибка подписки на топик %s: %v", topic, token.Error())
		} else {
			fmt.Printf("Подписка на топик %s успешна\n", topic)
		}
	}

	// Обработка сигналов завершения
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	fmt.Println("Завершение работы")
	client.Disconnect(250)
}
