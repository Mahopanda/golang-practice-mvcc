package mvcc_test

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Mahopanda/golang-mvcc/pkg/mvcc"
	"github.com/stretchr/testify/assert"
)

// 測試基礎功能：事務的寫入與讀取
func TestBasicTransaction(t *testing.T) {
	db := mvcc.NewDatabase()

	// 開始一個事務，寫入一筆資料
	tx1 := db.Begin(mvcc.ReadCommitted)
	t.Log("開始事務 tx1")
	
	err := db.Write(tx1, "key1", "value1")
	if err != nil {
		t.Fatalf("寫入失敗: %v", err)
	}
	t.Log("成功寫入 key1=value1")

	// 提交事務
	err = db.Commit(tx1)
	if err != nil {
		t.Fatalf("提交事務失敗: %v", err)
	}
	t.Log("成功提交事務 tx1")

	// 開始新事務進行讀取
	tx2 := db.Begin(mvcc.ReadCommitted)
	t.Log("開始新事務 tx2 進行讀取")

	// 驗證是否能成功讀取寫入的資料
	value, err := db.Read(tx2, "key1")
	if err != nil {
		t.Fatalf("讀取失敗: %v", err)
	}

	// 使用 assert 進行更清晰的錯誤檢查
	assert.Equal(t, "value1", value, "讀取值不符合預期: 預期值 'value1', 實際值 '%s'", value)
	
	// 檢查資料版本
	record := db.GetData()["key1"]
	if record == nil {
		t.Fatal("找不到 key1 的資料記錄")
	}
	
	versions := record.GetVersions()
	t.Logf("key1 的版本數量: %d", len(versions))
	for i, v := range versions {
		t.Logf("版本 %d: 值=%s, 時間戳=%d, 結束時間戳=%d", 
			i, v.Value, v.Timestamp, v.EndTS)
	}
}

// 測試隔離級別：Read Uncommitted
func TestReadUncommitted(t *testing.T) {
	db := mvcc.NewDatabase()

	// 事務 1 寫入資料
	tx1 := db.Begin(mvcc.ReadCommitted)
	db.Write(tx1, "key1", "value1")

	// 事務 2 嘗試讀取未提交的資料
	tx2 := db.Begin(mvcc.ReadCommitted)
	value, err := db.ReadWithIsolation(tx2, "key1", mvcc.ReadUncommitted)
	if err != nil || value != "value1" {
		t.Errorf("Read Uncommitted 測試失敗: 預期值 'value1', 實際值 '%s'", value)
	}
}

// 測試隔離級別：Read Committed
func TestReadCommitted(t *testing.T) {
	db := mvcc.NewDatabase()

	// 事務 1 寫入資料
	tx1 := db.Begin(mvcc.ReadCommitted)
	db.Write(tx1, "key1", "value1")

	// 事務 2 嘗試讀取，應該無法讀取未提交的資料
	tx2 := db.Begin(mvcc.ReadCommitted)
	value, err := db.ReadWithIsolation(tx2, "key1", mvcc.ReadCommitted)
	if err == nil && value != "" {
		t.Errorf("Read Committed 測試失敗: 未提交的資料不應該可見")
	}

	// 提交事務 1，然後事務 2 再次讀取
	db.Commit(tx1)
	value, err = db.ReadWithIsolation(tx2, "key1", mvcc.ReadCommitted)
	if err != nil || value != "value1" {
		t.Errorf("Read Committed 測試失敗: 預期值 'value1', 實際值 '%s'", value)
	}
}

// 測試隔離級別：Repeatable Read
func TestRepeatableRead(t *testing.T) {
	db := mvcc.NewDatabase()

	// 事務 1 寫入資料並提交
	tx1 := db.Begin(mvcc.ReadCommitted)
	db.Write(tx1, "key1", "value1")
	db.Commit(tx1)

	// 事務 2 開始，讀取快照
	tx2 := db.Begin(mvcc.ReadCommitted)
	value, err := db.ReadWithIsolation(tx2, "key1", mvcc.RepeatableRead)
	if err != nil || value != "value1" {
		t.Errorf("Repeatable Read 測試失敗: 預期值 'value1', 實際值 '%s'", value)
	}

	// 事務 3 修改資料
	tx3 := db.Begin(mvcc.ReadCommitted)
	db.Write(tx3, "key1", "value2")
	db.Commit(tx3)

	// 事務 2 再次讀取應該仍然看到舊快照
	value, err = db.ReadWithIsolation(tx2, "key1", mvcc.RepeatableRead)
	if err != nil || value != "value1" {
		t.Errorf("Repeatable Read 測試失敗: 預期值 'value1', 實際值 '%s'", value)
	}
}

// 測試多事務的並發寫入
func TestConcurrentTransactions(t *testing.T) {
	db := mvcc.NewDatabase()
	var wg sync.WaitGroup

	wg.Add(2)

	// 事務 1 寫入資料
	go func() {
		defer wg.Done()
		tx := db.Begin(mvcc.ReadCommitted)
		db.Write(tx, "key1", "value1")
		db.Commit(tx)
	}()

	// 事務 2 寫入資料
	go func() {
		defer wg.Done()
		tx := db.Begin(mvcc.ReadCommitted)
		db.Write(tx, "key1", "value2")
		db.Commit(tx)
	}()

	wg.Wait()

	// 驗證資料是否有其中一個寫入
	tx := db.Begin(mvcc.ReadCommitted)
	value, err := db.Read(tx, "key1")
	if err != nil || (value != "value1" && value != "value2") {
		t.Errorf("並發事務測試失敗: 實際值 '%s'", value)
	}
}

// 測試垃圾回收
func TestGarbageCollection(t *testing.T) {
	runtime.GOMAXPROCS(1) // 強制單執行緒執行
	db := mvcc.NewDatabase()

	t.Log("開始寫入第一個版本")
	tx1 := db.Begin(mvcc.ReadCommitted)
	db.Write(tx1, "key1", "value1")
	db.Commit(tx1)

	t.Log("模擬時間推進 5")
	db.AdvanceTime(5)

	t.Log("寫入第二個版本")
	tx2 := db.Begin(mvcc.ReadCommitted)
	db.Write(tx2, "key1", "value2")
	db.Commit(tx2)

	t.Log("模擬時間推進 6 到垃圾回收門檻後")
	db.AdvanceTime(6)

	t.Log("執行垃圾回收")
	db.CleanupOldVersions()

	record := db.GetData()["key1"]
	t.Log("檢查版本數量")
	if len(record.GetVersions()) != 1 {
		t.Errorf("垃圾回收測試失敗: 應該只保留 1 個版本，但實際保留 %d 個", len(record.GetVersions()))
	}

	// 確認保留的版本是最新版本
	latestVersion := record.GetVersions()[0]
	if latestVersion.Value != "value2" {
		t.Errorf("垃圾回收測試失敗: 最新版本應該是 'value2', 但實際是 '%s'", latestVersion.Value)
	}

	// 確認保留版本的 Timestamp
	if latestVersion.Timestamp != 7 {
		t.Errorf("垃圾回收測試失敗: 最新版本的 Timestamp 應該是 7, 但實際是 %d", latestVersion.Timestamp)
	}

	// 確認舊版本的 EndTS 被正確設置
	if len(record.GetVersions()) > 1 && record.GetVersions()[0].EndTS != 7 {
		t.Errorf("垃圾回收測試失敗: 舊版本的 EndTS 應該是 7, 但實際是 %d", record.GetVersions()[0].EndTS)
	}
}

func TestMVCC(t *testing.T) {
	db := mvcc.NewDatabase()

	// 測試基本寫入和讀取
	tx1 := db.Begin(mvcc.ReadCommitted)
	err := db.Write(tx1, "key1", "value1")
	assert.NoError(t, err)

	// 測試未提交讀取
	tx2 := db.Begin(mvcc.ReadCommitted)
	val, err := db.ReadWithIsolation(tx2, "key1", mvcc.ReadUncommitted)
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// 測試提交後讀取
	err = db.Commit(tx1)
	assert.NoError(t, err)

	tx3 := db.Begin(mvcc.ReadCommitted)
	val, err = db.ReadWithIsolation(tx3, "key1", mvcc.ReadCommitted)
	assert.NoError(t, err)
	assert.Equal(t, "value1", val)

	// 測試垃圾回收
	db.CleanupOldVersions()
	record := db.GetData()["key1"]
	assert.Equal(t, 1, len(record.GetVersions()))
}

// 測試事務回滾
func TestTransactionRollback(t *testing.T) {
	db := mvcc.NewDatabase()

	// 寫入初始數據
	tx1 := db.Begin(mvcc.ReadCommitted)
	err := db.Write(tx1, "key1", "initial")
	assert.NoError(t, err)
	err = db.Commit(tx1)
	assert.NoError(t, err)

	// 開始新事務並寫入
	tx2 := db.Begin(mvcc.ReadCommitted)
	err = db.Write(tx2, "key1", "modified")
	assert.NoError(t, err)

	// 讀取未提交的修改
	val, err := db.ReadWithIsolation(tx2, "key1", mvcc.ReadUncommitted)
	assert.NoError(t, err)
	assert.Equal(t, "modified", val)

	// 回滾事務
	err = db.Rollback(tx2)
	assert.NoError(t, err)

	// 驗證數據恢復到初始狀態
	tx3 := db.Begin(mvcc.ReadCommitted)
	val, err = db.Read(tx3, "key1")
	assert.NoError(t, err)
	assert.Equal(t, "initial", val)
}

// 測試並發讀寫衝突
func TestConcurrentReadWriteConflict(t *testing.T) {
	db := mvcc.NewDatabase()
	var wg sync.WaitGroup

	// 初始化數據
	tx1 := db.Begin(mvcc.ReadCommitted)
	db.Write(tx1, "key1", "initial")
	db.Commit(tx1)

	// 使用 channel 來控制事務執行順序
	startTx2 := make(chan struct{})
	wg.Add(2)

	// 事務1：讀取後修改
	go func() {
		defer wg.Done()
		tx := db.Begin(mvcc.RepeatableRead)
		
		// 讀取初始值
		val, err := db.ReadWithIsolation(tx, "key1", mvcc.RepeatableRead)
		assert.NoError(t, err)
		assert.Equal(t, "initial", val)

		// 通知事務2開始執行
		close(startTx2)
		
		// 模擬處理時間
		time.Sleep(50 * time.Millisecond)

		// 嘗試修改並提交
		err = db.Write(tx, "key1", "modified1")
		if err != nil {
			t.Logf("事務1寫入失敗：%v", err)
			return
		}
		
		err = db.Commit(tx)
		if err != nil {
			t.Logf("事務1提交失敗：%v", err)
			return
		}
	}()

	// 事務2：並發修改
	go func() {
		defer wg.Done()
		
		// 等待事務1完成讀取
		<-startTx2

		tx := db.Begin(mvcc.ReadCommitted)
		err := db.Write(tx, "key1", "modified2")
		if err != nil {
			t.Logf("事務2寫入失敗：%v", err)
			return
		}
		
		err = db.Commit(tx)
		if err != nil {
			t.Logf("事務2提交失敗：%v", err)
			return
		}
	}()

	wg.Wait()

	// 驗證最終結果
	tx := db.Begin(mvcc.ReadCommitted)
	val, err := db.Read(tx, "key1")
	assert.NoError(t, err)
	
	// 檢查最終值是否為其中之一的預期結果
	assert.True(t, val == "modified1" || val == "modified2", 
		"最終值應該是 'modified1' 或 'modified2'，實際值為：%s", val)
}

// 測試版本鏈完整性
func TestVersionChainIntegrity(t *testing.T) {
	db := mvcc.NewDatabase()

	// 創建多個版本
	versions := []string{"v1", "v2", "v3"}

	for _, v := range versions {
		tx := db.Begin(mvcc.ReadCommitted)
		err := db.Write(tx, "key1", v)
		assert.NoError(t, err)
		err = db.Commit(tx)
		assert.NoError(t, err)
	}

	// 檢查版本鏈
	record := db.GetData()["key1"]
	allVersions := record.GetVersions()
	assert.Equal(t, len(versions), len(allVersions))

	// 驗證時間戳遞增
	for i := 1; i < len(allVersions); i++ {
		assert.True(t, allVersions[i].Timestamp > allVersions[i-1].Timestamp)
	}

	// 驗證EndTS設置正確
	for i := 0; i < len(allVersions)-1; i++ {
		assert.Equal(t, allVersions[i+1].Timestamp, allVersions[i].EndTS)
	}

	// 驗證最後一個版本的EndTS為0
	assert.Equal(t, 0, allVersions[len(allVersions)-1].EndTS)
}

// 測試大量並發事務
func TestHighConcurrency(t *testing.T) {
	db := mvcc.NewDatabase()
	numGoroutines := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// 並發執行多個事務
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			tx := db.Begin(mvcc.ReadCommitted)
			key := fmt.Sprintf("key%d", id%10) // 使用10個不同的key
			value := fmt.Sprintf("value%d", id)

			err := db.Write(tx, key, value)
			assert.NoError(t, err)

			err = db.Commit(tx)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// 驗證數據一致性
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		tx := db.Begin(mvcc.ReadCommitted)
		_, err := db.Read(tx, key)
		assert.NoError(t, err)
	}
}

func TestDirtyRead(t *testing.T) {
	db := mvcc.NewDatabase()

	// T1: 寫入但不提交
	tx1 := db.Begin(mvcc.ReadUncommitted)
	err := db.Write(tx1, "key1", "value1")
	assert.NoError(t, err)

	// T2: 讀取未提交的數據
	tx2 := db.Begin(mvcc.ReadCommitted)
	_, err = db.Read(tx2, "key1")
	assert.Error(t, err) // 應該返回錯誤
}

func TestPhantomRead(t *testing.T) {
	db := mvcc.NewDatabase()

	// 初始化一些數據
	initTx := db.Begin(mvcc.ReadCommitted)
	db.Write(initTx, "key1", "value1")
	db.Write(initTx, "key3", "value3")
	db.Commit(initTx)

	// T1: 開始事務（使用 RepeatableRead 隔離級別）
	tx1 := db.Begin(mvcc.RepeatableRead)
	t.Log("T1: 開始第一次範圍讀取")
	
	// 第一次範圍讀取
	count1 := 0
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keys {
		val, err := db.ReadWithIsolation(tx1, key, mvcc.RepeatableRead)
		if err == nil && val != "" {
			count1++
		}
	}
	t.Logf("T1: 第一次讀取計數: %d", count1)

	// T2: 插入新數據
	t.Log("T2: 開始插入新數據")
	tx2 := db.Begin(mvcc.ReadCommitted)
	err := db.Write(tx2, "key4", "value4")
	assert.NoError(t, err)
	err = db.Commit(tx2)
	assert.NoError(t, err)
	t.Log("T2: 完成插入新數據")

	// T1: 第二次範圍讀取
	t.Log("T1: 開始第二次範圍讀取")
	count2 := 0
	for _, key := range keys {
		val, err := db.ReadWithIsolation(tx1, key, mvcc.RepeatableRead)
		if err == nil && val != "" {
			count2++
		}
	}
	t.Logf("T1: 第二次讀取計數: %d", count2)

	// 在 RepeatableRead 隔離級別下，兩次讀取應該看到相同的結果
	assert.Equal(t, count1, count2, 
		"在 RepeatableRead 隔離級別下，兩次讀取應該返回相同的結果")

	// 提交 T1 事務
	err = db.Commit(tx1)
	assert.NoError(t, err)

	// 驗證新的事務可以看到所有更改
	tx3 := db.Begin(mvcc.ReadCommitted)
	finalCount := 0
	for _, key := range keys {
		val, err := db.ReadWithIsolation(tx3, key, mvcc.ReadCommitted)
		if err == nil && val != "" {
			finalCount++
		}
	}
	t.Logf("最終讀取計數: %d", finalCount)
	assert.Equal(t, count1+1, finalCount, 
		"新事務應該能看到所有更改")
}
