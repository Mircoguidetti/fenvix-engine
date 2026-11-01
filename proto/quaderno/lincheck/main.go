// Controllo di linearizzabilità dello storico del banco 4 con porcupine (registro per chiave).
// Uso: go run . storico.csv [timeout_s]
// Righe: w,key,val,t0_us,t1_us,ok|unknown,pos  e  r,key,val,t0_us,t1_us,pos,replica,S,attempts
// Le scritture dall'esito ignoto restano "in volo per sempre" (ritorno = infinito).
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

type in struct {
	put bool
	key uint64
	val uint64
}
type out struct{ val uint64 }

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	timeout := 600
	if len(os.Args) > 2 {
		timeout, _ = strconv.Atoi(os.Args[2])
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var ops []porcupine.Operation
	var maxT int64
	nw, nr, nu := 0, 0, 0
	for sc.Scan() {
		p := strings.Split(sc.Text(), ",")
		u := func(i int) uint64 { x, _ := strconv.ParseUint(p[i], 10, 64); return x }
		t0, t1 := int64(u(3)), int64(u(4))
		if t1 > maxT {
			maxT = t1
		}
		if p[0] == "w" {
			nw++
			op := porcupine.Operation{ClientId: 0, Input: in{true, u(1), u(2)}, Call: t0, Output: out{}, Return: t1}
			if p[5] != "ok" {
				nu++
				op.Return = 1 << 62
			}
			ops = append(ops, op)
		} else {
			if u(2) == ^uint64(0) {
				continue // fuori finestra: non succede in questo banco
			}
			nr++
			ops = append(ops, porcupine.Operation{ClientId: 1, Input: in{false, u(1), 0}, Call: t0, Output: out{u(2)}, Return: t1})
		}
	}
	model := porcupine.Model{
		Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
			m := map[uint64][]porcupine.Operation{}
			for _, o := range h {
				k := o.Input.(in).key
				m[k] = append(m[k], o)
			}
			var r [][]porcupine.Operation
			for _, v := range m {
				r = append(r, v)
			}
			return r
		},
		Init: func() interface{} { return uint64(0) },
		Step: func(st, i, o interface{}) (bool, interface{}) {
			x := i.(in)
			if x.put {
				return true, x.val
			}
			return o.(out).val == st.(uint64), st
		},
		Equal: func(a, b interface{}) bool { return a.(uint64) == b.(uint64) },
	}
	start := time.Now()
	res := porcupine.CheckOperationsTimeout(model, ops, time.Duration(timeout)*time.Second)
	fmt.Printf("{\"check\": \"porcupine\", \"writes\": %d, \"writes_unknown\": %d, \"reads\": %d, \"result\": \"%v\", \"seconds\": %.1f}\n", nw, nu, nr, res, time.Since(start).Seconds())
	if res == porcupine.Illegal {
		os.Exit(1)
	}
}
