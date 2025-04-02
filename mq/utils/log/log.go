package log

import (
	"errors"
	"fmt"
	"os"
	"runtime"

	"github.com/ChewZ-life/go-pkg/mq/utils/tgbot"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/sirupsen/logrus"
)

type Log struct {
	log              *logrus.Logger
	tgbot            *tgbotapi.BotAPI
	tgChatId         int64
	logDir           string
	logName          string
	logModule        string
	logLevel         string
	logRotationTime  int  // Log file rotation frequency in hours
	logRotationCount uint // Number of log files to keep
}

type Option func(log *Log) error

func NewLog(name, logModule, tgBotToken string, tgChatId int64, options ...Option) (*Log, error) {
	instance := &Log{}
	instance.logName = name
	instance.logModule = logModule
	if tgBotToken != "" {
		instance.tgbot = tgbot.GetBot(tgBotToken)
	}
	instance.tgChatId = tgChatId

	// Set default values
	instance.log = logrus.New()
	instance.logDir = "./logs"
	instance.logRotationTime = 1
	instance.logRotationCount = 24
	instance.log.SetOutput(os.Stdout)

	// Set optional parameters
	for _, option := range options {
		if err := option(instance); err != nil {
			return nil, err
		}
	}

	instance.SetLogFormatter(&logrus.TextFormatter{})
	// hook := newLfsHook(instance.logDir, instance.logName, &instance.logLevel, instance.logRotationTime, instance.logRotationCount)
	// instance.log.SetOutput(&NilWriter{})
	// instance.log.AddHook(hook)
	return instance, nil
}

/*
SetLogDir sets the log directory
*/
func SetLogDir(dir string) Option {
	return func(log *Log) error {
		log.logDir = dir
		return nil
	}
}

/*
SetLogRotation sets the log rotation frequency and number of log files to keep
*/
func SetLogRotation(time int, count uint) Option {
	return func(log *Log) error {
		if time <= 0 || count <= 0 {
			return errors.New("set log rotation error")
		}
		log.logRotationTime = time
		log.logRotationCount = count
		return nil
	}
}

var logLevels = map[string]logrus.Level{
	"PanicLevel": logrus.PanicLevel,
	"FatalLevel": logrus.FatalLevel,
	"ErrorLevel": logrus.ErrorLevel,
	"WarnLevel":  logrus.WarnLevel,
	"InfoLevel":  logrus.InfoLevel,
	"DebugLevel": logrus.DebugLevel,
}

// Do not record logs to console
type NilWriter struct {
}

func (w *NilWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}

// Wrapper for logrus.Fields
type Fields logrus.Fields

func (log *Log) SetLogLevel(level string) {
	log.logLevel = level
}
func (log *Log) SetLogFormatter(formatter logrus.Formatter) {
	log.log.Formatter = formatter
}

// Debug
func (log *Log) Debug(args ...interface{}) {
	log.SetLogLevel("DebugLevel")
	entry := log.log.WithFields(logrus.Fields{})
	entry.Data["file"] = log.fileInfo(2)
	entry.Debug(args...)
}

// Debug with fields
func (log *Log) DebugWithFields(l interface{}, f Fields) {
	entry := log.log.WithFields(logrus.Fields(f))
	entry.Data["file"] = log.fileInfo(2)
	entry.Debug(l)
}

// Info
func (log *Log) Info(args ...interface{}) {
	log.SetLogLevel("InfoLevel")
	entry := log.log.WithFields(logrus.Fields{})
	entry.Data["file"] = log.fileInfo(2)
	entry.Info(args...)
}

// Info
func (log *Log) Infof(format string, args ...interface{}) {
	log.SetLogLevel("InfoLevel")
	entry := log.log.WithFields(logrus.Fields{})
	entry.Data["file"] = log.fileInfo(2)
	entry.Infof(format, args...)
}

// Info with fields
func (log *Log) InfoWithFields(l interface{}, f Fields) {
	entry := log.log.WithFields(logrus.Fields(f))
	entry.Data["file"] = log.fileInfo(2)
	entry.Info(l)
}

// Warn
func (log *Log) Warn(args ...interface{}) {
	log.SetLogLevel("WarnLevel")
	entry := log.log.WithFields(logrus.Fields{})
	entry.Data["file"] = log.fileInfo(2)
	entry.Warn(args...)
}

// Warn with fields
func (log *Log) WarnWithFields(l interface{}, f Fields) {
	entry := log.log.WithFields(logrus.Fields(f))
	entry.Data["file"] = log.fileInfo(2)
	entry.Warn(l)
}

// ErrMsg
func (log *Log) Error(args ...interface{}) {
	log.SetLogLevel("ErrorLevel")
	entry := log.log.WithFields(logrus.Fields{})
	entry.Data["file"] = log.fileInfo(2)
	entry.Error(args...)
	log.SendToTG("ERROR", args, nil)
}

// Error with fields
func (log *Log) ErrorWithFields(l interface{}, f Fields) {
	entry := log.log.WithFields(logrus.Fields(f))
	entry.Data["file"] = log.fileInfo(2)
	entry.Error(l)
	log.SendToTG("ERROR", l, f)
}

// Fatal
func (log *Log) Fatal(args ...interface{}) {
	log.SetLogLevel("FatalLevel")
	entry := log.log.WithFields(logrus.Fields{})
	entry.Data["file"] = log.fileInfo(2)
	log.SendToTG("FATAL", args, nil)
	entry.Fatal(args...)
}

// Fatal with fields
func (log *Log) FatalWithFields(l interface{}, f Fields) {
	entry := log.log.WithFields(logrus.Fields(f))
	entry.Data["file"] = log.fileInfo(2)
	log.SendToTG("FATAL", l, f)

	entry.Fatal(l)
}

// Panic
func (log *Log) Panic(args ...interface{}) {
	log.SetLogLevel("PanicLevel")
	entry := log.log.WithFields(logrus.Fields{})
	entry.Data["file"] = log.fileInfo(2)
	log.SendToTG("PANIC", args, nil)

	entry.Panic(args...)
}

// Panic with fields
func (log *Log) PanicWithFields(l interface{}, f Fields) {
	entry := log.log.WithFields(logrus.Fields(f))
	entry.Data["file"] = log.fileInfo(2)
	log.SendToTG("PANIC", l, f)
	entry.Panic(l)
}

func (log *Log) fileInfo(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		file = "<???>"
		line = 1
	}
	return fmt.Sprintf("%s:%d", file, line)
}

func (log *Log) SendToTG(level string, msg, fields interface{}) {
	msgStr := fmt.Sprintf("[pm-%s] module=%s level=%v msg=%v fields=%+v", log.logName, log.logModule, level, msg, fields)
	if log.tgbot != nil && log.tgChatId != 0 {
		log.tgbot.Send(tgbotapi.NewMessage(log.tgChatId, msgStr))
	}
}
