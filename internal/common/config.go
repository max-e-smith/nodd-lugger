package common

import "github.com/spf13/viper"

func GetWorkersConfig() int {
	numWorkers := viper.GetInt("parallel")
	if numWorkers < 1 {
		return 1
	}
	if numWorkers > 100 {
		return 100
	}
	return numWorkers
}
