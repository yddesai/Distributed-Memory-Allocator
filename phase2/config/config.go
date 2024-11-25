package config

const (
	AWS_PORT            = 8080
	DEFAULT_LAPTOP_PORT = 8081
	HEARTBEAT_INTERVAL  = 5  // seconds
	NODE_TIMEOUT        = 15 // seconds
	BACKUP_FOLDER       = "aws_backup"
	LOCAL_FOLDER        = "received_files"
)

type Config struct {
	AWSURL     string
	ChunkSize  int64 // in bytes
	MaxRetries int
	Timeout    int // in seconds
}

var DefaultConfig = Config{
	AWSURL:     "http://18.144.165.108:8080",
	ChunkSize:  8 * 1024 * 1024, // 8MB
	MaxRetries: 3,
	Timeout:    30,
}
