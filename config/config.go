package config

import "github.com/spf13/viper"

type Config struct {
	Kafka Kafka
}

type Kafka struct {
	Brokers   []string `yaml:"Brokers"`
	Topic     string   `yaml:"Topic"`
	Partition int      `yaml:"Partition"`
	GroupID   string   `yaml:"GroupID"`
	WriteTo   string   `yaml:"WriteTo"`
}

func ReadConfig(cfgName, cfgType, cfgPath string) *Config {
	var cfg Config

	viper.SetConfigName(cfgName)
	viper.SetConfigType(cfgType)
	viper.AddConfigPath(cfgPath)

	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	if err := viper.Unmarshal(&cfg); err != nil {
		panic(err)
	}

	return &cfg
}
