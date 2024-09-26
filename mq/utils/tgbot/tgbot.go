package tgbot

import (
	"sync"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var once sync.Once
var bot *tgbotapi.BotAPI

func GetBot(token string) *tgbotapi.BotAPI {
	once.Do(func() {
		var err error
		bot, err = tgbotapi.NewBotAPI(token)
		if err != nil {
			panic(err)
		}
	})

	return bot
}
