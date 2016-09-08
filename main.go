// xlsxtomysql project main.go
package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tealeg/xlsx"
	"golang.org/x/crypto/bcrypt"

	_ "github.com/go-sql-driver/mysql"
)

var (
	db      *sql.DB
	columns []string
	err     error
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("请按照格式输入：xlsxtomysql [DSN] [数据库名称] [*.xlsx]")
		os.Exit(-1)
	}

	dsn := os.Args[1]
	tableName := os.Args[2]
	fileName := os.Args[3]

	db, err = sql.Open("mysql", dsn)
	checkerr(err)
	db.SetMaxOpenConns(2000)
	db.SetMaxIdleConns(1000)
	defer db.Close()

	rows, err := db.Query(`SELECT * FROM ` + tableName)
	checkerr(err)

	//获取数据表字段名
	columns, err = rows.Columns()
	checkerr(err)
	rows.Close()

	xlFile, err := xlsx.OpenFile(fileName)
	if err != nil {
		checkerr(err)
	}

	usecolumns := make([]string, len(xlFile.Sheets[0].Rows[0].Cells))

	//对比Excel与数据表中字段
	for i := 0; i < len(xlFile.Sheets[0].Rows[0].Cells); i++ {
		thiscolumn, _ := xlFile.Sheets[0].Rows[0].Cells[i].String()
		xlsxcolumn := strings.Split(thiscolumn, "|")
		if xlsxcolumn[0] == ":other" {
			usecolumns[i] = ":other"
		} else {
			for _, value := range columns {
				if xlsxcolumn[0] == value {
					usecolumns[i] = value
				}
			}
		}
	}

	if len(usecolumns) < 1 {
		fmt.Println("数据表与xlsx表格不对应")
		os.Exit(-1)
	}

	ch := make(chan string)

	for i := 1; i < len(xlFile.Sheets[0].Rows); i++ {
		go func(usecolumns []string, i int, db *sql.DB, ch chan string) {
			insertvalue := make(map[string]string)
			insertsql := `INSERT INTO ` + tableName + ` SET `
			tmp := 0
			var ot []string
			var othercolumns []string
			for key, value := range usecolumns {
				insertvalue[value], _ = xlFile.Sheets[0].Rows[i].Cells[key].String()
				thiscolumn, _ := xlFile.Sheets[0].Rows[0].Cells[key].String()
				xlsxcolumn := strings.Split(thiscolumn, "|")

				//解析内容
				if xlsxcolumn[0] == ":other" {
					ot = strings.Split(insertvalue[value], "|")
					rows, err := db.Query("SELECT * FROM " + ot[0])
					checkerr(err)
					othercolumns, err = rows.Columns()
					rows.Close()
					checkerr(err)
				} else {
					if len(xlsxcolumn) > 1 {
						switch xlsxcolumn[1] {
						case "unique":
							result, _ := fetchRow(db, `SELECT count(`+xlsxcolumn[0]+`) as has FROM `+tableName+` WHERE `+xlsxcolumn[0]+` = '`+insertvalue[value]+`'`)
							has, _ := strconv.Atoi((*result)["has"])
							if has > 0 {
								fmt.Println(xlsxcolumn[0] + ":" + insertvalue[value] + "重复，自动跳过")
								ch <- "error"
								return
							}
						case "password":
							tmpvalue := strings.Split(insertvalue[value], "|")
							if len(tmpvalue) == 2 {
								if []byte(tmpvalue[1])[0] == ':' {
									if _, ok := insertvalue[string([]byte(tmpvalue[1])[1:])]; ok {
										insertvalue[value] = tmpvalue[0] + insertvalue[string([]byte(tmpvalue[1])[1:])]
									} else {
										fmt.Println("密码盐" + string([]byte(tmpvalue[1])[1:]) + "字段不存在，自动跳过")
										ch <- "error"
										return
									}
								} else {
									insertvalue[value] += tmpvalue[1]
								}
							} else {
								insertvalue[value] = tmpvalue[0]
							}
							switch xlsxcolumn[2] {
							case "md5":
								insertvalue[value] = string(md5.New().Sum([]byte(insertvalue[value])))
							case "bcrypt":
								pass, _ := bcrypt.GenerateFromPassword([]byte(insertvalue[value]), 13)
								insertvalue[value] = string(pass)
							}
						case "find":
							result, _ := fetchRow(db, `SELECT `+xlsxcolumn[3]+` FROM `+xlsxcolumn[2]+` WHERE `+xlsxcolumn[4]+` = '`+insertvalue[value]+`'`)
							if (*result)["id"] == "" {
								fmt.Println("表 " + xlsxcolumn[2] + " 中没有找到 " + xlsxcolumn[4] + " 为 " + insertvalue[value] + " 的数据，自动跳过")
								ch <- "error"
								return
							}
							insertvalue[value] = (*result)["id"]

						}
					}
					insertvalue[value] = getVal(insertvalue[value])

					if tmp == 0 {
						insertsql += value + ` = '` + insertvalue[value] + `'`
					} else {
						insertsql += `, ` + value + ` = '` + insertvalue[value] + `'`
					}
					tmp++

				}
			}

			smt, err := db.Prepare(insertsql + `;`)
			defer smt.Close()
			checkerr(err)

			//执行附表操作
			if len(ot) > 0 {
				res, err := smt.Exec()
				id, _ := res.LastInsertId()

				otinsertsql := `INSERT INTO ` + ot[0] + ` SET `
				for i := 0; i < len(othercolumns); i++ {
					ot[i+1] = getVal(ot[i+1])
					if ot[i+1] == ":id" {
						ot[i+1] = strconv.Itoa(int(id))
					}
					if i == 0 {
						otinsertsql += othercolumns[i] + ` = '` + ot[i+1] + `'`
					} else {
						otinsertsql += `, ` + othercolumns[i] + ` = '` + ot[i+1] + `'`

					}
				}
				otsmt, err := db.Prepare(otinsertsql + `;`)
				defer otsmt.Close()
				checkerr(err)
				_, err = otsmt.Exec()
				checkerr(err)
			} else {
				_, err = smt.Exec()
			}
			checkerr(err)
			ch <- "success"
		}(usecolumns, i, db, ch)
	}

	for i := 1; i < len(xlFile.Sheets[0].Rows); i++ {
		<-ch
	}

}

//解析内容
func getVal(val string) string {
	switch val {
	case ":time":
		return strconv.Itoa(int(time.Now().Unix()))
	case ":random":
		return strings.Replace(substr(base64.StdEncoding.EncodeToString(Krand(32, KC_RAND_KIND_ALL)), 0, 32), "+/", "_-", -1)
	default:
		return val
	}
}

//按长度截取字符串
func substr(str string, start int, length int) string {
	rs := []rune(str)
	rl := len(rs)
	end := 0

	if start < 0 {
		start = rl - 1 + start
	}
	end = start + length

	if start > end {
		start, end = end, start
	}

	if start < 0 {
		start = 0
	}
	if start > rl {
		start = rl
	}
	if end < 0 {
		end = 0
	}
	if end > rl {
		end = rl
	}

	return string(rs[start:end])
}
