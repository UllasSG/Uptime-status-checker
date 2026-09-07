package config

import (
	"encoding/json"
	"os"
)

type Target struct {
	Name             string `json:"name"`
	URL              string `json:"url"`
	IntervalSecs     int    `json:"interval_secs"`
	TimeoutSecs      int    `json:"timeout_secs"`
	ExpectedStatus   []int  `json:"expected_status"`
	FailureThreshold int    `json:"failure_threshold"`
}

type Config struct {
	Workers      int      `json:"workers"`
	DbPath       string   `json:"db_path"`
	RetentionHrs int      `json:"retention_hrs"`
	Targets      []Target `json:"targets"`
}

func Load(path string) (Config, error) {
	config := Config{}
	file, err := os.Open(path)
	if err != nil {
		return config, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		return config, err
	}
	return config, nil
}
