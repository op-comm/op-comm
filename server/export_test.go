package server

import "sync"

/*
This file is for accessing the private methods for test only purposes.
For example we want to test that our manager successfully removes and manages sessions to prevent
memory leaks however accessing sessions should not be public functionality
*/


func GetManagerSessionCount(manager *Manager) int{
	return manager.sessionCount()
}

func GetManagerSessionMutex(manager *Manager) *sync.RWMutex{
	return &manager.sessionMutex
}

func GetManagerSessionMap(manager *Manager) map[string]*Session {
	return manager.sessions
}

func RemoveSessionFromAllManagerRooms(manager *Manager, session *Session){
	manager.removeSessionFromAllRooms(session)
}
