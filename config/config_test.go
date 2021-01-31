package config

import (
	"fmt"
	"testing"
)

func TestInitConfig(t *testing.T) {
	config := InitConfig("C:\\Users\\16678\\zpkv\\config\\c1.json")
	fmt.Println(config)
}
