package goToolRedis

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"time"
)

type Redis struct{
	config *RedisConfig
	pool *redis.Pool
}

type RedisConfig struct{
	Server      string
	Port int
	Auth        string
	// 最大空闲连接数
	MaxIdle     int
	// 一个pool所能分配的最大的连接数目
	// 当设置成0的时候，该pool连接数没有限制
	MaxActive   int
	// 空闲连接超时时间，超过超时时间的空闲连接会被关闭。
	// 如果设置成0，空闲连接将不会被关闭
	// 应该设置一个比redis服务端超时时间更短的时间
	IdleTimeout int
}

func NewRedis(config *RedisConfig) *Redis{
	r := &Redis{
		config:config,
	}
	r.pool = r.newPool()
	return r
}

//创建连接池
func (r *Redis) newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     r.config.MaxIdle,
		MaxActive:   r.config.MaxActive,
		IdleTimeout: time.Duration(1000 * 1000 * 1000 * r.config.IdleTimeout),
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", r.config.Server + ":" + strconv.Itoa(r.config.Port))
			if err != nil {
				return nil, err
			}
			_, err = c.Do("auth", r.config.Auth)
			if err != nil {
				errLs := c.Close()
				if errLs != nil {
					fmt.Println(errLs)
				}
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

//插入
func (r *Redis) Set(db int, key string, value string) (result string, err error) {
	conn := r.pool.Get()
	defer func() {
		errLs := conn.Close()
		if errLs != nil {
			fmt.Println(errLs)
		}
	}()
	_, err = conn.Do("SELECT", db)
	if err != nil {
		return
	}
	result, err = redis.String(conn.Do("SET", key, value))
	return
}

//查询
func (r *Redis) Get(db int, key string) (result string, err error) {
	conn := r.pool.Get()
	//defer func(){
	//	err = conn.Close()
	//	if err != nil {
	//		global.MyLog(err.Error())
	//	}
	//}()
	defer func() {
		errLs := conn.Close()
		if errLs != nil {
			fmt.Println(errLs)
		}
	}()
	_, err = conn.Do("SELECT", db)
	if err != nil {
		return
	}
	result, err = redis.String(conn.Do("GET", key))
	return
}

//检查是否存在
func (r *Redis) IsExists(db int, key string) (result bool, err error) {
	conn := r.pool.Get()
	defer func() {
		errLs := conn.Close()
		if errLs != nil {
			fmt.Println(errLs)
		}
	}()
	_, err = conn.Do("SELECT", db)
	if err != nil {
		return
	}
	result, err = redis.Bool(conn.Do("EXISTS", key))
	return
}

//删除
func (r *Redis) Del(db int, key string) (err error) {
	conn := r.pool.Get()
	defer func() {
		errLs := conn.Close()
		if errLs != nil {
			fmt.Println(errLs)
		}
	}()
	_, err = conn.Do("SELECT", db)
	if err != nil {
		return
	}
	_, err = conn.Do("DEL", key)
	return
}

//关闭
func (r *Redis) Close(){
	if r.pool != nil {
		_ = r.pool.Close()
	}
	r.pool = nil
}