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

//列
type columns struct {
	xlsxColumns  []*xlsx.Cell
	tableColumns []string
	useColumns   [][]string
}

//行
type row struct {
	insertID int64
	sql      string
	value    map[string]string
	ot       *otherTable
}

//附表
type otherTable struct {
	sql     string
	value   []string
	columns []string
}

func main() {
	c := new(columns)
	if len(os.Args) != 4 {
		fmt.Println("请按照格式输入：xlsxtomysql [DSN] [数据库名称] [*.xlsx]")
		os.Exit(-1)
	}

	dsn := os.Args[1]
	tableName := os.Args[2]
	fileName := os.Args[3]

	db, err := sql.Open("mysql", dsn)
	checkerr(err)
	defer db.Close()
	db.SetMaxOpenConns(2000)
	db.SetMaxIdleConns(1000)

	rows, err := db.Query("SELECT * FROM " + tableName + " LIMIT 1")
	checkerr(err)
	//获取数据表字段名
	c.tableColumns, err = rows.Columns()
	checkerr(err)
	rows.Close()

	xlFile, err := xlsx.OpenFile(fileName)
	if err != nil {
		checkerr(err)
	}

	c.xlsxColumns = xlFile.Sheets[0].Rows[0].Cells

	c.paraseColumns()

	ch := make(chan string, 50)
	rowsnum := len(xlFile.Sheets[0].Rows)
	for i := 1; i < rowsnum; i++ {
		go func(c *columns, i int, db *sql.DB, ch chan string) {

			r := &row{value: make(map[string]string), sql: "INSERT INTO `" + tableName + "` SET ", ot: new(otherTable)}
			tmp := 0
			for key, value := range c.useColumns {
				r.value[value[0]], _ = xlFile.Sheets[0].Rows[i].Cells[key].String()

				//解析内容
				if value[0] == ":other" {
					r.ot.value = strings.Split(r.value[value[0]], "|")
					rows, err := db.Query("SELECT * FROM " + r.ot.value[0])
					checkerr(err)
					r.ot.columns, err = rows.Columns()
					rows.Close()
					checkerr(err)
				} else {
					r.ot = nil
					if len(value) > 1 {
						switch value[1] {
						case "unique":
							result, _ := fetchRow(db, "SELECT count("+value[0]+") as has FROM `"+tableName+"` WHERE `"+value[0]+"` = '"+r.value[value[0]]+"'")
							has, _ := strconv.Atoi((*result)["has"])
							if has > 0 {
								fmt.Print(value[0] + ":" + r.value[value[0]] + "重复，自动跳过\n")
								ch <- "error"
								return
							}
						case "password":
							tmpvalue := strings.Split(r.value[value[0]], "|")
							if len(tmpvalue) == 2 {
								if []byte(tmpvalue[1])[0] == ':' {
									if _, ok := r.value[string([]byte(tmpvalue[1])[1:])]; ok {
										r.value[value[0]] = tmpvalue[0] + r.value[string([]byte(tmpvalue[1])[1:])]
									} else {
										fmt.Print("密码盐" + string([]byte(tmpvalue[1])[1:]) + "字段不存在，自动跳过\n")
										ch <- "error"
										return
									}
								} else {
									r.value[value[0]] += tmpvalue[1]
								}
							} else {
								r.value[value[0]] = tmpvalue[0]
							}
							switch value[2] {
							case "md5":
								r.value[value[0]] = string(md5.New().Sum([]byte(r.value[value[0]])))
							case "bcrypt":
								pass, _ := bcrypt.GenerateFromPassword([]byte(r.value[value[0]]), 13)
								r.value[value[0]] = string(pass)
							}
						case "find":
							result, _ := fetchRow(db, "SELECT `"+value[3]+"` FROM `"+value[2]+"` WHERE "+value[4]+" = '"+r.value[value[0]]+"'")
							if (*result)["id"] == "" {
								fmt.Print("表 " + value[2] + " 中没有找到 " + value[4] + " 为 " + r.value[value[0]] + " 的数据，自动跳过\n")
								ch <- "error"
								return
							}
							r.value[value[0]] = (*result)["id"]

						}
					}
					r.value[value[0]] = paraseValue(r.value[value[0]])

					if r.value[value[0]] != "" {
						if tmp == 0 {
							r.sql += "`" + value[0] + "` = '" + r.value[value[0]] + "'"
						} else {
							r.sql += ", `" + value[0] + "` = '" + r.value[value[0]] + "'"
						}
						tmp++
					}

				}
			}

			smt, err := db.Prepare(r.sql + ";")
			defer smt.Close()
			checkerr(err)

			res, err := smt.Exec()
			r.insertID, _ = res.LastInsertId()
			checkerr(err)

			//执行附表操作
			if r.ot != nil {
				r.ot.sql = "INSERT INTO `" + r.ot.value[0] + "` SET "
				tmp = 0
				for key, value := range r.ot.columns {
					r.ot.value[key+1] = paraseValue(r.ot.value[key+1])
					if r.ot.value[key+1] == ":id" {
						r.ot.value[key+1] = strconv.Itoa(int(r.insertID))
					}
					if r.ot.value[key+1] != "" {
						if tmp == 0 {
							r.ot.sql += "`" + value + "` = '" + r.ot.value[key+1] + "'"
						} else {
							r.ot.sql += ", `" + value + "` = '" + r.ot.value[key+1] + "'"

						}
						tmp++
					}
				}
				otsmt, err := db.Prepare(r.ot.sql + ";")
				checkerr(err)
				defer otsmt.Close()
				_, err = otsmt.Exec()
				checkerr(err)
			}
			ch <- "success"
		}(c, i, db, ch)
	}

	for i := 1; i < rowsnum; i++ {
		if <-ch == "success" {
			fmt.Println("[" + strconv.Itoa(i) + "/" + strconv.Itoa(rowsnum-1) + "]导入数据成功")
		}
	}

}

//解析Excel及数据库字段
func (c *columns) paraseColumns() {
	c.useColumns = make([][]string, len(c.xlsxColumns))
	for key, value := range c.xlsxColumns {
		thiscolumn, _ := value.String()
		columnval := strings.Split(thiscolumn, "|")
		if columnval[0] == ":other" {
			c.useColumns[key] = columnval
		} else {
			for _, value := range c.tableColumns {
				if columnval[0] == value {
					c.useColumns[key] = columnval
				}
			}
		}
	}
	if len(c.useColumns) < 1 {
		fmt.Println("数据表与xlsx表格不对应")
		os.Exit(-1)
	}
}

//解析内容
func paraseValue(val string) string {
	switch val {
	case ":null":
		return ""
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
