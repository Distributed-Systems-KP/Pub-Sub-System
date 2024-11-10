package broker

import (
	"database/sql"

	_ "modernc.org/sqlite" // SQLite driver
)

type Storage struct {
	db *sql.DB
}

func NewStorage(dbPath string) (*Storage, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	storage := &Storage{db: db}
	err = storage.initTables()
	if err != nil {
		return nil, err
	}
	return storage, nil
}

func (s *Storage) initTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS topics (
            name TEXT PRIMARY KEY
        );`,
		`CREATE TABLE IF NOT EXISTS partitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT,
            broker_id TEXT,
            FOREIGN KEY(topic) REFERENCES topics(name)
        );`,
		`CREATE TABLE IF NOT EXISTS brokers (
            id TEXT PRIMARY KEY,
            address TEXT
        );`,
	}

	for _, query := range queries {
		_, err := s.db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) AddBroker(id, address string) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO brokers (id, address) VALUES (?, ?)`, id, address)
	return err
}

func (s *Storage) GetBrokers() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT id, address FROM brokers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	brokers := make(map[string]string)
	for rows.Next() {
		var id, address string
		if err := rows.Scan(&id, &address); err != nil {
			return nil, err
		}
		brokers[id] = address
	}
	return brokers, nil
}

func (s *Storage) AddTopic(name string) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO topics (name) VALUES (?)`, name)
	return err
}

func (s *Storage) GetTopics() ([]string, error) {
	rows, err := s.db.Query(`SELECT name FROM topics`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var topics []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		topics = append(topics, name)
	}
	return topics, nil
}

func (s *Storage) AddPartition(topic string, brokerID string) (int, error) {
	result, err := s.db.Exec(`INSERT INTO partitions (topic, broker_id) VALUES (?, ?)`, topic, brokerID)
	if err != nil {
		return 0, err
	}
	id, err := result.LastInsertId()
	return int(id), err
}

func (s *Storage) GetPartitions(topic string) ([]int, error) {
	rows, err := s.db.Query(`SELECT id FROM partitions WHERE topic = ?`, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		partitions = append(partitions, id)
	}
	return partitions, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}
