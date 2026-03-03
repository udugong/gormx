package connpool

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync/atomic"

	"gorm.io/gorm"
)

var errUnknownPattern = errors.New("未知的双写模式")

type DoubleWritePool struct {
	src     gorm.ConnPool
	dst     gorm.ConnPool
	pattern atomic.Value
	logger  *slog.Logger
}

func NewDoubleWritePool(src *gorm.DB, dst *gorm.DB, logger *slog.Logger) *DoubleWritePool {
	pool := &DoubleWritePool{
		src:    src.ConnPool,
		dst:    dst.ConnPool,
		logger: logger,
	}
	pool.pattern.Store(PatternSrcOnly)
	return pool
}

func (p *DoubleWritePool) UpdatePattern(pattern string) error {
	switch pattern {
	case PatternSrcOnly, PatternSrcFirst, PatternDstFirst, PatternDstOnly:
		p.pattern.Store(pattern)
		return nil
	default:
		return errUnknownPattern
	}
}

func (p *DoubleWritePool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	pattern := p.pattern.Load().(string)
	switch pattern {
	case PatternSrcOnly:
		src, err := p.src.(gorm.TxBeginner).BeginTx(ctx, opts)
		return &DoubleWriteTx{src: src, logger: p.logger, pattern: pattern}, err
	case PatternSrcFirst:
		src, err := p.src.(gorm.TxBeginner).BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		dst, err := p.dst.(gorm.TxBeginner).BeginTx(ctx, opts)
		if err != nil {
			p.logger.LogAttrs(ctx, slog.LevelError, "双写目标表开启事务失败", slog.Any("err", err))
		}
		return &DoubleWriteTx{src: src, dst: dst, logger: p.logger, pattern: pattern}, nil
	case PatternDstFirst:
		dst, err := p.dst.(gorm.TxBeginner).BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		src, err := p.src.(gorm.TxBeginner).BeginTx(ctx, opts)
		if err != nil {
			p.logger.LogAttrs(ctx, slog.LevelError, "双写源表开启事务失败", slog.Any("err", err))
		}
		return &DoubleWriteTx{src: src, dst: dst, logger: p.logger, pattern: pattern}, nil
	case PatternDstOnly:
		dst, err := p.dst.(gorm.TxBeginner).BeginTx(ctx, opts)
		return &DoubleWriteTx{dst: dst}, err
	default:
		return nil, errUnknownPattern
	}
}

func (p *DoubleWritePool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	panic("双写模式写不支持")
}

func (p *DoubleWritePool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	switch p.pattern.Load().(string) {
	case PatternSrcOnly:
		return p.src.ExecContext(ctx, query, args...)
	case PatternSrcFirst:
		res, err := p.src.ExecContext(ctx, query, args...)
		if err == nil {
			_, err1 := p.dst.ExecContext(ctx, query, args...)
			if err1 != nil {
				p.logger.LogAttrs(ctx, slog.LevelError, "双写写入 dst 失败",
					slog.String("query", query), slog.Any("err", err1))
			}
		}
		return res, err
	case PatternDstFirst:
		res, err := p.dst.ExecContext(ctx, query, args...)
		if err == nil {
			_, err1 := p.src.ExecContext(ctx, query, args...)
			if err1 != nil {
				p.logger.LogAttrs(ctx, slog.LevelError, "双写写入 src 失败",
					slog.String("query", query), slog.Any("err", err1))
			}
		}
		return res, err
	case PatternDstOnly:
		return p.dst.ExecContext(ctx, query, args...)
	default:
		return nil, errUnknownPattern
	}
}

func (p *DoubleWritePool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	switch p.pattern.Load().(string) {
	case PatternSrcOnly, PatternSrcFirst:
		return p.src.QueryContext(ctx, query, args...)
	case PatternDstOnly, PatternDstFirst:
		return p.dst.QueryContext(ctx, query, args...)
	default:

		return nil, errUnknownPattern
	}
}

func (p *DoubleWritePool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	switch p.pattern.Load().(string) {
	case PatternSrcOnly, PatternSrcFirst:
		return p.src.QueryRowContext(ctx, query, args...)
	case PatternDstOnly, PatternDstFirst:
		return p.dst.QueryRowContext(ctx, query, args...)
	default:
		// 这样你没有带上错误信息
		// return &sql.Row{}
		panic(errUnknownPattern)
	}
}

type DoubleWriteTx struct {
	src     *sql.Tx
	dst     *sql.Tx
	pattern string
	logger  *slog.Logger
}

func (tx *DoubleWriteTx) Commit() error {
	switch tx.pattern {
	case PatternSrcOnly:
		return tx.src.Commit()
	case PatternSrcFirst:
		err := tx.src.Commit()
		if err != nil {
			return err
		}
		if tx.dst != nil {
			err1 := tx.dst.Commit()
			if err1 != nil {
				tx.logger.Error("目标表提交事务失败")
			}
		}
		return nil
	case PatternDstFirst:
		err := tx.dst.Commit()
		if err != nil {
			return err
		}
		if tx.src != nil {
			err1 := tx.src.Commit()
			if err1 != nil {
				tx.logger.Error("源表提交事务失败")
			}
		}
		return nil
	case PatternDstOnly:
		return tx.dst.Commit()
	default:
		return errUnknownPattern
	}
}

func (tx *DoubleWriteTx) Rollback() error {
	switch tx.pattern {
	case PatternSrcOnly:
		return tx.src.Rollback()
	case PatternSrcFirst:
		err := tx.src.Rollback()
		if err != nil {
			return err
		}
		if tx.dst != nil {
			err1 := tx.dst.Rollback()
			if err1 != nil {
				// 你只能是记录日志
				tx.logger.Error("目标表回滚事务失败")
			}
		}
		return nil
	case PatternDstFirst:
		err := tx.dst.Rollback()
		if err != nil {
			return err
		}
		if tx.src != nil {
			err1 := tx.src.Rollback()
			if err1 != nil {
				// 你只能是记录日志
				tx.logger.Error("源表回滚事务失败")
			}
		}
		return nil
	case PatternDstOnly:
		return tx.dst.Rollback()
	default:
		return errUnknownPattern
	}
}

func (tx *DoubleWriteTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	panic("双写模式写不支持")
}

func (tx *DoubleWriteTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	switch tx.pattern {
	case PatternSrcOnly:
		return tx.src.ExecContext(ctx, query, args...)
	case PatternSrcFirst:
		res, err := tx.src.ExecContext(ctx, query, args...)
		if err == nil && tx.dst != nil {
			_, err1 := tx.dst.ExecContext(ctx, query, args...)
			if err1 != nil {
				tx.logger.LogAttrs(ctx, slog.LevelError, "双写写入 dst 失败",
					slog.String("query", query), slog.Any("err", err1))
			}
		}
		return res, err
	case PatternDstFirst:
		res, err := tx.dst.ExecContext(ctx, query, args...)
		if err == nil && tx.src != nil {
			_, err1 := tx.src.ExecContext(ctx, query, args...)
			if err1 != nil {
				tx.logger.LogAttrs(ctx, slog.LevelError, "双写写入 src 失败",
					slog.String("query", query), slog.Any("err", err1))
			}
		}
		return res, err
	case PatternDstOnly:
		return tx.dst.ExecContext(ctx, query, args...)
	default:
		return nil, errUnknownPattern
	}
}

func (tx *DoubleWriteTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	switch tx.pattern {
	case PatternSrcOnly, PatternSrcFirst:
		return tx.src.QueryContext(ctx, query, args...)
	case PatternDstOnly, PatternDstFirst:
		return tx.dst.QueryContext(ctx, query, args...)
	default:

		return nil, errUnknownPattern
	}
}

func (tx *DoubleWriteTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	switch tx.pattern {
	case PatternSrcOnly, PatternSrcFirst:
		return tx.src.QueryRowContext(ctx, query, args...)
	case PatternDstOnly, PatternDstFirst:
		return tx.dst.QueryRowContext(ctx, query, args...)
	default:
		// 这样你没有带上错误信息
		// return &sql.Row{}
		panic(errUnknownPattern)
	}
}

const (
	PatternSrcOnly  = "src_only"
	PatternSrcFirst = "src_first"
	PatternDstFirst = "dst_first"
	PatternDstOnly  = "dst_only"
)
