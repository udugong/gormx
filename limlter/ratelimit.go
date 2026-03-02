package limlter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"gorm.io/gorm"
)

var ErrGormTooManyRequests = errors.New("gorm 请求过多")

// OperationType 定义操作类型
type OperationType string

const (
	OperationCreate OperationType = "create"
	OperationUpdate OperationType = "update"
	OperationDelete OperationType = "delete"
	OperationQuery  OperationType = "query"
	OperationRaw    OperationType = "raw"
	OperationRow    OperationType = "row"
)

type Limiter interface {
	// Limit 有没有触发限流。key 就是限流对象
	// bool 代表是否限流, true 就是要限流
	// err 限流器本身有没有错误
	Limit(ctx context.Context, key string) (bool, error)
}

type RateLimitCallbacks struct {
	globalLimiter Limiter                   // 全局的限流器,若没有分别配置则统一都用这个
	limiters      map[OperationType]Limiter // 根据不同的操作使用不同的限流器
	logger        *slog.Logger
}

func NewRateLimitCallbacks(l Limiter, logger *slog.Logger,
	opts ...RateLimitCallbacksOption) *RateLimitCallbacks {
	c := &RateLimitCallbacks{
		globalLimiter: l,
		limiters:      make(map[OperationType]Limiter, 6),
		logger:        logger,
	}
	for _, opt := range opts {
		opt.apply(c)
	}
	return c
}

// WithOperationLimiter 按操作类型设置限流器
func WithOperationLimiter(op OperationType, l Limiter) RateLimitCallbacksOption {
	return rateLimitCallbacksOptionFunc(func(c *RateLimitCallbacks) {
		c.limiters[op] = l
	})
}

// WithOperationLimiters 批量设置操作类型限流器
func WithOperationLimiters(limiters map[OperationType]Limiter) RateLimitCallbacksOption {
	return rateLimitCallbacksOptionFunc(func(c *RateLimitCallbacks) {
		c.limiters = limiters
	})
}

func (c *RateLimitCallbacks) Name() string {
	return "rate_limiter"
}

func (c *RateLimitCallbacks) Initialize(db *gorm.DB) error {
	err := db.Callback().Create().Before("*").
		Register("rate_limiter_before_create", c.beforeCreate)
	if err != nil {
		return err
	}

	err = db.Callback().Update().Before("*").
		Register("rate_limiter_before_update", c.beforeUpdate)
	if err != nil {
		return err
	}

	err = db.Callback().Delete().Before("*").
		Register("rate_limiter_before_delete", c.beforeDelete)
	if err != nil {
		return err
	}

	err = db.Callback().Query().Before("*").
		Register("rate_limiter_before_query", c.beforeQuery)
	if err != nil {
		return err
	}

	err = db.Callback().Raw().Before("*").
		Register("rate_limiter_before_raw", c.beforeRaw)
	if err != nil {
		return err
	}

	err = db.Callback().Row().Before("*").
		Register("rate_limiter_before_row", c.beforeRow)
	if err != nil {
		return err
	}
	return nil
}

func (c *RateLimitCallbacks) limit(db *gorm.DB, op OperationType) {
	ctx := db.Statement.Context
	l := c.globalLimiter
	suffix := ""
	if limiter, ok := c.limiters[op]; ok {
		l = limiter
		suffix = string(op)
	}
	limited, err := l.Limit(ctx, c.key(suffix))
	if err != nil {
		c.logger.LogAttrs(ctx, slog.LevelError, "限流器异常", slog.Any("err", err))
		limited = true
	}
	if limited {
		_ = db.AddError(fmt.Errorf("%s 操作触发限流; err: %v", op, ErrGormTooManyRequests))
	}
}

func (c *RateLimitCallbacks) key(suffix string) string {
	const baseKey = "gorm_db_limiter"
	var b strings.Builder
	b.Grow(22)
	b.WriteString(baseKey)
	if suffix != "" {
		b.WriteString("_")
		b.WriteString(suffix)
	}
	return b.String()
}

func (c *RateLimitCallbacks) beforeCreate(db *gorm.DB) {
	c.limit(db, OperationCreate)
}

func (c *RateLimitCallbacks) beforeUpdate(db *gorm.DB) {
	c.limit(db, OperationUpdate)
}

func (c *RateLimitCallbacks) beforeDelete(db *gorm.DB) {
	c.limit(db, OperationDelete)
}

func (c *RateLimitCallbacks) beforeQuery(db *gorm.DB) {
	c.limit(db, OperationQuery)
}

func (c *RateLimitCallbacks) beforeRaw(db *gorm.DB) {
	c.limit(db, OperationRaw)
}

func (c *RateLimitCallbacks) beforeRow(db *gorm.DB) {
	c.limit(db, OperationRow)
}

type RateLimitCallbacksOption interface {
	apply(*RateLimitCallbacks)
}

type rateLimitCallbacksOptionFunc func(*RateLimitCallbacks)

func (f rateLimitCallbacksOptionFunc) apply(r *RateLimitCallbacks) {
	f(r)
}
