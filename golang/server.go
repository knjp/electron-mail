package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/mail"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jhillyerd/enmime"
	_ "github.com/mattn/go-sqlite3"
)

type SearchResult struct {
	ID          int    `json:"id"`
	Subject     string `json:"subject"`
	Body        string `json:"body"`
	Date        string `json:"date"`
	IsImportant int    `json:"is_important"`
	HasUrgent   int    `json:"has_urgent"`
	calDate     string `json:"cal_date"`
	location    string `json:"location"`
}

var progressChan = make(chan string)

func main() {
	db, err := sql.Open("sqlite3", "db/mail_index.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// テーブルの初期化（FTS5 + メタデータカラム）
	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec(`CREATE VIRTUAL TABLE IF NOT EXISTS mails USING fts5(
		subject, body, path, date, is_important, has_urgent, from_addr, cal_date, location
	);`)
	db.Exec(`CREATE TABLE IF NOT EXISTS file_cache (
		path TEXT PRIMARY KEY, mtime INTEGER, size INTEGER
	);`)

	// スキャン開始
	go scanTargetDirWithProgress(db, "./Mailfolders")

	// 検索API
	http.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		query := r.URL.Query().Get("q")
		dateRange := r.URL.Query().Get("range")

		cutoffDate := time.Now()
		switch dateRange {
		case "1w":
			cutoffDate = cutoffDate.AddDate(0, 0, -7)
		case "1m":
			cutoffDate = cutoffDate.AddDate(0, -1, 0)
		case "6m":
			cutoffDate = cutoffDate.AddDate(0, -6, 0)
		case "1y":
			cutoffDate = cutoffDate.AddDate(-1, 0, 0)
		case "5y":
			cutoffDate = cutoffDate.AddDate(-5, 0, 0)
		case "10y":
			cutoffDate = cutoffDate.AddDate(-10, 0, 0)
		case "all":
			cutoffDate = time.Time{} // nil time で全件対象
		default:
			cutoffDate = cutoffDate.AddDate(-1, 0, 0)
		}
		// SQL の WHERE 句を構築
		/*
			var dateFilter string
			if dateRange != "all" {
				// SQLite の date() 関数を使って比較
				dateFilter = fmt.Sprintf(" AND date >= '%s'", cutoffDate.Format("2006-01-02 15:04:05"))
			}
		*/

		keywords := strings.Fields(query)
		ftsQuery := ""
		for _, k := range keywords {
			ftsQuery += `"` + k + `*"` + " "
		}

		cutoffStr := cutoffDate.Format("2006-01-02 15:04:05")
		var rows *sql.Rows
		var err error

		if strings.TrimSpace(query) == "" {
			// 検索ワードがない場合：日付条件だけで全件（最新1000件）取得
			finalQuery := `
        SELECT rowid, subject, body, date, is_important, has_urgent 
        FROM mails 
        WHERE date >= ? 
        ORDER BY has_urgent DESC, is_important DESC, date DESC 
        LIMIT 1000`
			rows, err = db.Query(finalQuery, cutoffStr)
		} else {
			// 検索ワードがある場合：日付 ＋ キーワード
			finalQuery := `
        SELECT rowid, subject, body, date, is_important, has_urgent 
        FROM mails 
        WHERE date >= ? AND mails MATCH ? 
        ORDER BY has_urgent DESC, is_important DESC, date DESC 
        LIMIT 1000`
			rows, err = db.Query(finalQuery, cutoffStr, ftsQuery)
		}

		/*
			finalQuery := `
			    SELECT rowid, subject, body, date, is_important, has_urgent
			    FROM mails
			    WHERE mails MATCH ? AND date >= ?
			    ORDER BY has_urgent DESC, is_important DESC, date DESC
			    LIMIT 1000`
			rows, err := db.Query(finalQuery, ftsQuery, cutoffStr) // 2つの ? に値を渡す
		*/

		if err != nil {
			json.NewEncoder(w).Encode([]SearchResult{})
			return
		}
		defer rows.Close()

		results := []SearchResult{}
		for rows.Next() {
			var res SearchResult
			rows.Scan(&res.ID, &res.Subject, &res.Body, &res.Date, &res.IsImportant, &res.HasUrgent, &res.calDate, &res.location)
			results = append(results, res)
		}
		json.NewEncoder(w).Encode(results)
	})

	// 進捗SSE
	http.HandleFunc("/progress", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		for msg := range progressChan {
			fmt.Fprintf(w, "data: %s\n\n", msg)
			w.(http.Flusher).Flush()
		}
	})

	log.Println("Backend API started at :8090")
	log.Fatal(http.ListenAndServe(":8090", nil))
}

func isAllowed(path string) bool {
	//targets := []string{"gmail-4.com", "office365", "Local Folders"}
	targets := []string{"gmail"}
	lowerPath := strings.ToLower(path)
	for _, t := range targets {
		if strings.Contains(lowerPath, strings.ToLower(t)) {
			return true
		}
	}
	return false
}

func countTotalAllowedFiles(root string) int {
	count := 0
	var walk func(string)
	walk = func(p string) {
		files, _ := os.ReadDir(p)
		for _, f := range files {
			fp := filepath.Join(p, f.Name())
			info, _ := f.Info()
			if info.Mode()&os.ModeSymlink != 0 {
				resolved, _ := filepath.EvalSymlinks(fp)
				resInfo, _ := os.Stat(resolved)
				if resInfo.IsDir() {
					walk(resolved)
					continue
				}
				info = resInfo
			}
			if info.IsDir() {
				walk(fp)
			} else if !strings.HasSuffix(fp, ".msf") && isAllowed(fp) {
				count++
			}
		}
	}
	walk(root)
	return count
}

func scanTargetDirWithProgress(db *sql.DB, targetPath string) {
	realPath, _ := filepath.EvalSymlinks(targetPath)
	total := countTotalAllowedFiles(realPath)
	current := 0

	var walk func(string)
	walk = func(p string) {
		files, _ := os.ReadDir(p)
		for _, f := range files {
			fp := filepath.Join(p, f.Name())
			info, _ := f.Info()
			if info.Mode()&os.ModeSymlink != 0 {
				res, _ := filepath.EvalSymlinks(fp)
				resInf, _ := os.Stat(res)
				if resInf.IsDir() {
					walk(res)
					continue
				}
				info = resInf
			}
			if info.IsDir() {
				walk(fp)
			} else if !strings.HasSuffix(fp, ".msf") && isAllowed(fp) {
				current++
				progressChan <- fmt.Sprintf("(%d/%d) 更新中: %s", current, total, filepath.Base(fp))
				processFile(db, fp, info)
			}
		}
	}
	walk(realPath)
	progressChan <- "同期完了"
}

func processFile(db *sql.DB, path string, info os.FileInfo) {
	var lastM, lastS int64
	err := db.QueryRow("SELECT mtime, size FROM file_cache WHERE path = ?", path).Scan(&lastM, &lastS)
	if err == nil && lastM == info.ModTime().Unix() && lastS == info.Size() {
		return
	}

	db.Exec("DELETE FROM mails WHERE path = ?", path)
	tx, _ := db.Begin()
	indexMboxFile(tx, path)
	tx.Commit()
	db.Exec("INSERT OR REPLACE INTO file_cache VALUES (?, ?, ?)", path, info.ModTime().Unix(), info.Size())
}

func indexMboxFile(tx *sql.Tx, filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var currentMail strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if strings.HasPrefix(line, "From ") || err == io.EOF {
			if currentMail.Len() > 0 {
				r := strings.NewReader(currentMail.String())
				env, _ := enmime.ReadEnvelope(r)
				if env != nil {
					rawDate := env.GetHeader("Date")
					parsedDate, err := mail.ParseDate(rawDate) // net/mail パッケージを使用

					dateForDB := rawDate // パース失敗時のフォールバック
					if err == nil {
						// SQLiteが正しく比較・ソートできる形式 (年-月-日 時:分:秒)
						dateForDB = parsedDate.Format("2006-01-02 15:04:05")
					}

					imp, urg, calDate, location := analyzeMail(env)
					tx.Exec(`INSERT INTO mails(subject, body, path, date, is_important, has_urgent, from_addr, calDate, location) 
						VALUES (?, ?, ?, ?, ?, ?, ?)`,
						env.GetHeader("Subject"), env.Text, filePath, dateForDB, imp, urg, env.GetHeader("From"), calDate, location)
				}
				currentMail.Reset()
			}
			if err == io.EOF {
				break
			}
		}
		currentMail.WriteString(line)
	}
}

func analyzeMail(env *enmime.Envelope) (isImp int, hasUrg int, calDate string, location string) {
	sub, body, from := env.GetHeader("Subject"), env.Text, env.GetHeader("From")

	// --- 1. 重要度判定 ---
	vips := []string{"boss@example.com", "@important.jp"}
	for _, v := range vips {
		if strings.Contains(strings.ToLower(from), v) {
			isImp = 1
		}
	}
	keywords := []string{"重要", "緊急", "締切", "確定"}
	for _, k := range keywords {
		if strings.Contains(sub, k) {
			isImp = 1
		}
	}

	// --- 2. 日付の抽出と「年」の補完 ---
	now := time.Now()
	// パターン: 「1月30日」や「01/30」など
	reDate := regexp.MustCompile(`(\d{1,2})[月/](\d{1,2})日?`)
	match := reDate.FindStringSubmatch(body)

	if len(match) > 0 {
		hasUrg = 1
		month, _ := strconv.Atoi(match[1])
		day, _ := strconv.Atoi(match[2])

		// 年の推測：現在の月より大幅に前の月（例：11月に1月のメール）なら「来年」とみなす
		year := now.Year()
		if time.Month(month) < now.Month()-1 {
			year++
		}
		// Googleカレンダー形式 (YYYYMMDD)
		calDate = fmt.Sprintf("%04d%02d%02d", year, month, day)
	}

	// --- 3. 場所の抽出 ---
	// パターンA: 「場所：〇〇」「会場：〇〇」
	// パターンB: 「〇〇会議室」「〇〇ビル」などのキーワード
	reLoc := regexp.MustCompile(`(?:場所|会場|集合)[:：]\s*([^\n\r]+)|([^\n\r ]+(?:会議室|ホール|センター|ビル|スタジオ))`)
	locMatch := reLoc.FindStringSubmatch(body)
	if len(locMatch) > 0 {
		// マッチしたグループ (1番目か2番目) から中身がある方を採用
		for i := 1; i < len(locMatch); i++ {
			if locMatch[i] != "" {
				location = strings.TrimSpace(locMatch[i])
				break
			}
		}
	}

	return
}

func analyzeMail_old(env *enmime.Envelope) (isImp int, hasUrg int, calDate string, location string) {
	sub, body, from := env.GetHeader("Subject"), env.Text, env.GetHeader("From")
	vips := []string{"boss@example.com", "@important.jp"}
	for _, v := range vips {
		if strings.Contains(strings.ToLower(from), v) {
			isImp = 1
		}
	}
	keywords := []string{"重要", "緊急", "締切", "確定"}
	for _, k := range keywords {
		if strings.Contains(sub, k) {
			isImp = 1
		}
	}

	now := time.Now()
	oneWeek := now.Add(7 * 24 * time.Hour)
	re := regexp.MustCompile(`(\d{1,2})月(\d{1,2})日`)
	matches := re.FindAllStringSubmatch(body, -1)
	for _, m := range matches {
		mo, _ := strconv.Atoi(m[1])
		d, _ := strconv.Atoi(m[2])
		event := time.Date(now.Year(), time.Month(mo), d, 0, 0, 0, 0, time.Local)
		if event.After(now.AddDate(0, 0, -1)) && event.Before(oneWeek) {
			hasUrg = 1
			break
		}
	}

	// 日時パターンの抽出 (例: 1月30日 15:00)
	reDate := regexp.MustCompile(`(\d{1,2}月\d{1,2}日)\s*(\d{1,2}[:時]\d{0,2}分?)?`)
	if reDate.MatchString(body) {
		hasUrg = 1 // 日付があればフラグを立てる
	}

	// 場所パターンの抽出 (例: 第1会議室, 〇〇ビル, 3F)
	// 日本語の特性上、100%は難しいですが、代表的なキーワードで拾います
	reLocation := regexp.MustCompile(`(場所|会場|集合)[:：]\s*([^\n\r]+)|([^\n\r]+(?:会議室|ホール|センター|ビル|スタジオ))`)
	_ = reLocation.FindString(body) // 今回はフラグ判定のみ利用

	return
}
