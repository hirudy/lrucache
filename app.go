package main

import (
	"fmt"
	"hash/crc32"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// 缓存存储基本节点
type Node struct {
	Key  string
	Data interface{}
	ExpireTime time.Time

	PreNode *Node
	NextNode *Node
}

// 缓存节点 (双链表 + hashmap)
type LRUCache struct {
	head  *Node
	tail  *Node

	keyMap map[string]*Node

	size int64
	len  int64

	maxLock sync.Mutex
}

// 获取缓存
func (l *LRUCache) GetCache(key string) interface{} {
	if l.keyMap[key] == nil {
		return nil
	}

	var currentNode *Node
	func() {
		l.maxLock.Lock()
		defer l.maxLock.Unlock()
		currentNode = l.keyMap[key]
		l.removeToTop(currentNode)
	}()

	// 如果已经过期了返回空
	if currentNode.ExpireTime.Before(time.Now()) {
		l.DeleteCache(key)
		return nil
	}
	return currentNode.Data
}

// 设置缓存
func (l *LRUCache) SetCache(key string, value interface{}, expireTime time.Time)  {
	l.maxLock.Lock()
	defer l.maxLock.Unlock()

	if l.keyMap[key] == nil {
		newNode := Node{
			Key: key,
			Data: value,
			ExpireTime: expireTime,
			PreNode: nil,
			NextNode: l.head}

		l.keyMap[key] = &newNode
		if l.head == nil {
			l.head = &newNode
			l.tail = l.head
			l.len = 0
		} else {
			l.head.PreNode = &newNode
			l.head = &newNode
		}
		l.len ++
	} else {
		currentNode := l.keyMap[key]
		currentNode.Data = value
		l.removeToTop(currentNode)
	}

	l.clearTail()
}

// 清理过期节点
func (l *LRUCache) DeleteCache(key string){
	if l.keyMap[key] == nil {
		return
	}
	currentNode := l.keyMap[key]
	l.maxLock.Lock()
	defer l.maxLock.Unlock()


	// 清空
	delete(l.keyMap, currentNode.Key)
	l.len --

	preNode := currentNode.PreNode
	nextNode := currentNode.NextNode
	currentNode.PreNode = nil
	currentNode.NextNode = nil

	// 如果是头部节点
	if preNode == nil {
		l.head = nextNode
		if l.head == nil {
			return
		}
		l.head.PreNode = nil
		return
	}

	// 如果是尾部节点
	if nextNode  == nil {
		l.tail = preNode
		if l.tail == nil {
			return
		}
		l.tail.NextNode = nil
		return
	}

	// 中间节点
	preNode.NextNode = nextNode
	nextNode.PreNode = preNode
}

func (l *LRUCache) Scan(){
	currentNode := l.head
	for ;; {
		if currentNode == nil {
			break
		}

		if currentNode.ExpireTime.After(time.Now()) {
			//fmt.Printf("%s, %s, %s \n", currentNode.Key,currentNode.Data, currentNode.ExpireTime)
		}
		currentNode = currentNode.NextNode
	}
	//fmt.Println()
}

// 移动头部节点
func (l *LRUCache) removeToTop(currentNode *Node)  {
	if l.len <= 1 {
		return
	}
	if currentNode.PreNode == nil {
		return
	}

	// 剥离current Node
	currentNode.PreNode.NextNode =  currentNode.NextNode
	if currentNode.NextNode != nil {
		currentNode.NextNode.PreNode = currentNode.PreNode
	}

	// 将 当前节点移动到 头部
	l.head.PreNode = currentNode
	currentNode.NextNode = l.head
	currentNode.PreNode = nil
	l.head = currentNode
}

// 清理尾部节点
func (l *LRUCache) clearTail()  {
	if l.len <= l.size {
		return
	}
	needClearLen := l.len - l.size
	for i:= int64(0); i < needClearLen; i++ {
		currentNode := l.tail

		delete(l.keyMap, currentNode.Key)
		l.tail = l.tail.PreNode
		l.tail.NextNode = nil
		currentNode.PreNode = nil
		l.len --
	}
}

// 清理过期节点
func (l *LRUCache) clearExpire()  {
	currentNode := l.head
	for ;; {
		if currentNode == nil {
			break
		}

		if currentNode.ExpireTime.Before(time.Now()) {
			l.DeleteCache(currentNode.Key)
		}
		currentNode = currentNode.NextNode
	}
}

func NewLRUCache(size int64) *LRUCache  {
	cache := LRUCache{
		head: nil,
		tail: nil,

		keyMap: map[string]*Node{},
		size: size,
		len: int64(0),
	}

	//go func() {
	//	for ;; {
	//		time.Sleep(1 * time.Second)
	//		cache.clearExpire()
	//	}
	//}()
	return &cache
}

type ConcurrentCache struct {
	lruCacheList map[int]*LRUCache
	size int
	len  int
	bucketMaxNum int

	lock sync.Mutex
}

func (c *ConcurrentCache) getBucketNum(key string) int {
	crcNum := crc32.ChecksumIEEE([]byte(key))
	return int(crcNum) % c.bucketMaxNum
}

// 获取缓存
func (c *ConcurrentCache) GetCache(key string) interface{} {
	if key == "" {
		return nil
	}
	lruCache := c.lruCacheList[c.getBucketNum(key)]
	if lruCache != nil {
		return lruCache.GetCache(key)
	}
	return nil
}

// 设置缓存
func (c *ConcurrentCache) SetCache(key string, value interface{}, expireTime time.Time)  {
	if key == "" {
		return
	}
	lruCache := c.lruCacheList[c.getBucketNum(key)]
	if lruCache == nil {
		func () {
			c.lock.Lock()
			defer c.lock.Unlock()
			lruCache = c.lruCacheList[c.getBucketNum(key)]
			if lruCache == nil {
				lruCache = NewLRUCache(int64(50 + c.size/c.bucketMaxNum))
				c.lruCacheList[c.getBucketNum(key)] = lruCache
			}
		}()
	}
	lruCache.SetCache(key, value, expireTime)
}

// 清理过期节点
func (c *ConcurrentCache) DeleteCache(key string){
	if key == "" {
		return
	}
	lruCache := c.lruCacheList[c.getBucketNum(key)]
	if lruCache == nil {
		return
	}
	lruCache.DeleteCache(key)
}

func (c *ConcurrentCache) Scan(){
	keys := reflect.ValueOf(c.lruCacheList).MapKeys()
	for key := range keys{
		lruCache := c.lruCacheList[key]
		if lruCache == nil {
			continue
		}
		lruCache.Scan()
	}
}

func NewConcurrentCache(size int, bucketNum int) *ConcurrentCache {
	cache := ConcurrentCache{
		lruCacheList: map[int]*LRUCache{},
		size: size,
		len: 0,
		bucketMaxNum: bucketNum,
	}

	go func() {
		for ;; {
			time.Sleep(5 * time.Second)
		 	var keys []int
			func(){
				cache.lock.Lock()
				defer cache.lock.Unlock()
				for key := range cache.lruCacheList {
					keys = append(keys, key)
				}
			}()

			for _, key := range keys{
				if cache.lruCacheList[key] == nil {
					continue
				}
				cache.lruCacheList[key].clearExpire()
				if cache.lruCacheList[key].len <= 0 {
					func() {
						cache.lock.Lock()
						defer cache.lock.Unlock()
						if cache.lruCacheList[key].len > 0 {
							return
						}
						delete(cache.lruCacheList, key)
					}()
				}
			}
		}
	}()

	return &cache
}



func main()  {
	cache := NewConcurrentCache(100, 10)

	for i:=0; i < 8; i++ {
		iStr := strconv.Itoa(i)
		cache.SetCache(iStr, "rudy tan" + iStr, time.Unix(time.Now().Unix() + 5 + int64(i) * 3, 0))
	}

	fmt.Printf("get %s \n", cache.GetCache("0"))
	fmt.Printf("get %s \n", cache.GetCache("7"))
	fmt.Printf("get %s \n", cache.GetCache("6"))
	fmt.Println()

	for ;; {
		//cache.Scan()
		fmt.Println()
		fmt.Printf("get %s \n", cache.GetCache("0"))
		fmt.Printf("get %s \n", cache.GetCache("7"))
		fmt.Printf("get %s \n", cache.GetCache("6"))
		time.Sleep(1 * time.Second)
	}

}
