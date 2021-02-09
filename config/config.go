package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	Local        string   `json:"local"`
	Peers        []string `json:"peers"`
	DbPath       string   `json:"db_path"`
	ServerMaster string   `json:"server_master"`
}

func InitConfig(fileName string) *Config {
	var c Config
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(bytes, &c)
	if err != nil {
		log.Fatal(err)
	}

	return &c
}
