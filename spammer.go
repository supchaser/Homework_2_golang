package main

import (
	"fmt"
	"log"
	"slices"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	if len(cmds) == 0 {
		return
	}

	channels := make([]chan interface{}, len(cmds)+1)
	bufferSize := 10
	for i := range channels {
		channels[i] = make(chan interface{}, bufferSize)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(cmds))

	for i, cmd := range cmds {
		go func() {
			defer wg.Done()
			cmd(channels[i], channels[i+1])
			close(channels[i+1])
		}()
	}

	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	uniqueUsers := make(map[uint64]string)

	for userID := range in {
		wg.Add(1)
		go func() {
			defer wg.Done()

			userIDStr, ok := userID.(string)
			if !ok {
				log.Printf("Invalid type for userID: %T", userID)
			}

			person := GetUser(userIDStr)
			mu.Lock()
			defer mu.Unlock()

			if _, ok := uniqueUsers[person.ID]; !ok {
				uniqueUsers[person.ID] = person.Email
				out <- person
			}
		}()
	}

	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	var wg sync.WaitGroup
	var mu sync.Mutex

	batchLen := GetMessagesMaxUsersBatch
	container := make([]User, 0, batchLen)

	userToMsgID := func(users []User) {
		defer wg.Done()

		msgID, err := GetMessages(users...)
		if err != nil {
			log.Printf("Ошибка в GetMessages: %v", err)
			return
		}

		for _, id := range msgID {
			out <- id
		}
	}

	mainUserToMsgID := func(user interface{}) {
		mu.Lock()
		defer mu.Unlock()

		userCastInUser, ok := user.(User)
		if !ok {
			log.Printf("Invalid type for user: %T", user)
			return
		}

		container = append(container, userCastInUser)
		if len(container) == batchLen {
			containerCopy := make([]User, len(container))
			copy(containerCopy, container)
			container = make([]User, 0, batchLen)

			wg.Add(1)
			go userToMsgID(containerCopy)
		}
	}

	for user := range in {
		mainUserToMsgID(user)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(container) > 0 {
		wg.Add(1)
		go userToMsgID(container)
	}

	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	goRoutinesNum := HasSpamMaxAsyncRequests
	wg := sync.WaitGroup{}

	for i := 0; i < goRoutinesNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msgID := range in {
				msgIDCastInMsgID, ok := msgID.(MsgID)
				if !ok {
					log.Printf("Invalid type for msgID: %T", msgID)
					continue
				}

				var msgData MsgData
				var err error

				msgData.ID = msgIDCastInMsgID
				msgData.HasSpam, err = HasSpam(msgData.ID)
				if err != nil {
					log.Printf("Ошибка в CheckSpam: %v", err)
					continue
				}

				out <- msgData
			}
		}()
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	const capacityContainer = 10
	container := make([]MsgData, 0, capacityContainer)

	for msgData := range in {
		container = append(container, msgData.(MsgData))
	}

	slices.SortFunc(container, func(a, b MsgData) int {
		if a.HasSpam == b.HasSpam {
			if a.ID < b.ID {
				return -1
			} else if a.ID > b.ID {
				return 1
			}
			return 0
		}
		if a.HasSpam {
			return -1
		}
		return 1
	})

	for _, msgData := range container {
		out <- fmt.Sprintf("%t %d", msgData.HasSpam, msgData.ID)
	}
}
