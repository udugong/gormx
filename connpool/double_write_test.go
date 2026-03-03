package connpool

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type DoubleWriteTestSuite struct {
	suite.Suite
	db  *gorm.DB
	src *gorm.DB
	dst *gorm.DB
}

func (s *DoubleWriteTestSuite) SetupSuite() {
	t := s.T()
	src, err := gorm.Open(mysql.Open("root:123456@tcp(localhost:13306)/double_write_test"))
	require.NoError(t, err)
	err = src.AutoMigrate(&DoubleWriteTest{})
	require.NoError(t, err)
	dst, err := gorm.Open(mysql.Open("root:123456@tcp(localhost:13306)/double_write_test_new"))
	require.NoError(t, err)
	err = dst.AutoMigrate(&DoubleWriteTest{})
	require.NoError(t, err)
	pool := NewDoubleWritePool(src, dst, slog.New(slog.NewTextHandler(os.Stdout, nil)))
	pool.pattern.Store(PatternSrcFirst)
	doubleWrite, err := gorm.Open(mysql.New(mysql.Config{
		Conn: pool,
	}))
	require.NoError(t, err)
	s.db = doubleWrite
	s.src = src
	s.dst = dst
}

func (s *DoubleWriteTestSuite) TearDownTest() {
	s.db.Exec("TRUNCATE TABLE double_write_tests")
}

func (s *DoubleWriteTestSuite) TestDoubleWriteTest() {
	t := s.T()
	baseData := DoubleWriteTest{
		Id:    1,
		BizId: 10086,
		Biz:   "test",
	}
	err := s.db.Create(&baseData).Error
	assert.NoError(t, err)
	var srcData DoubleWriteTest
	assert.NoError(t, s.src.Where(DoubleWriteTest{Id: 1}).First(&srcData).Error)
	assert.Equal(t, baseData, srcData)
	var dstData DoubleWriteTest
	assert.NoError(t, s.dst.Where(DoubleWriteTest{Id: 1}).First(&dstData).Error)
	assert.Equal(t, baseData, dstData)
}

func (s *DoubleWriteTestSuite) TestDoubleWriteTransaction() {
	t := s.T()
	baseData := DoubleWriteTest{
		Id:    2,
		BizId: 10087,
		Biz:   "test",
	}
	err := s.db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&baseData).Error
	})
	require.NoError(t, err)
	var srcData DoubleWriteTest
	assert.NoError(t, s.src.Where(DoubleWriteTest{Id: 2}).First(&srcData).Error)
	assert.Equal(t, baseData, srcData)
	var dstData DoubleWriteTest
	assert.NoError(t, s.dst.Where(DoubleWriteTest{Id: 2}).First(&dstData).Error)
	assert.Equal(t, baseData, dstData)
}

func TestDoubleWrite(t *testing.T) {
	suite.Run(t, new(DoubleWriteTestSuite))
}

type DoubleWriteTest struct {
	Id    int64  `gorm:"primaryKey,autoIncrement"`
	BizId int64  `gorm:"uniqueIndex:biz_type_id"`
	Biz   string `gorm:"type:varchar(128);uniqueIndex:biz_type_id"`
}
