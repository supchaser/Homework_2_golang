package main

import (
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	if len(cmds) == 0 {
		return
	}

	channels := make([]chan interface{}, len(cmds)+1)
	for i := range channels {
		channels[i] = make(chan interface{})
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
	m := make(map[uint64]string)
	for str := range in {
		wg.Add(1)
		go func() {
			defer wg.Done()
			person := GetUser(str.(string))
			mu.Lock()
			defer mu.Unlock()
			if _, ok := m[person.ID]; !ok {
				m[person.ID] = person.Email
				out <- person
			}
		}()
	}

	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	wg := sync.WaitGroup{}
	container := make([]User, 0, 2)
	mu := sync.Mutex{}
	for user := range in {
		mu.Lock()
		container = append(container, user.(User))
		if len(container) == 2 {
			currentUser1, currentUser2 := container[0], container[1]
			container = make([]User, 0, 2)
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				msgID, err := GetMessages(currentUser1, currentUser2)
				if err != nil {
					fmt.Println("Ошибка в GetMessages: ", err)
					return
				}
				for _, id := range msgID {
					out <- id
				}
			}()
		} else {
			mu.Unlock()
		}
	}

	mu.Lock()
	if len(container) == 1 {
		currentUser := container[0]
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgID, err := GetMessages(currentUser)
			if err != nil {
				fmt.Println("Ошибка в GetMessages: ", err)
				return
			}
			for _, id := range msgID {
				out <- id
			}
		}()
	} else {
		mu.Unlock()
	}

	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	const goRoutinesNum = 5
	workerInput := make(chan MsgID)
	wg := sync.WaitGroup{}

	for i := 0; i < goRoutinesNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msgID := range workerInput {
				var msgData MsgData
				var err error
				msgData.ID = msgID
				msgData.HasSpam, err = HasSpam(msgData.ID)
				if err != nil {
					fmt.Println("Ошибка в CheckSpam: ", err)
					continue
				}
				out <- msgData
			}
		}()
	}

	go func() {
		defer close(workerInput)
		for id := range in {
			workerInput <- id.(MsgID)
		}
	}()

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	container := make([]MsgData, 0)

	for msgData := range in {
		container = append(container, msgData.(MsgData))
	}

	sort.Slice(container, func(i, j int) bool {
		if container[i].HasSpam == container[j].HasSpam {
			return container[i].ID < container[j].ID
		}
		return container[i].HasSpam
	})

	for _, msgData := range container {
		out <- fmt.Sprintf("%t %d", msgData.HasSpam, msgData.ID)
	}
}
