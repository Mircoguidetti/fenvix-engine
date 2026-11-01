package main

// Modello per la prova "delta + esiste" in gara: per ogni chiave un documento che può esistere (con un contatore) o
// no. put v: esiste con valore v. del: non esiste. inc ok: esisteva, +1. inc nomatch: non esisteva (nessun effetto).
// read: valore o assente. Le operazioni dall'esito ignoto restano in volo per sempre e possono avere o no effetto.

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

type eIn struct {
	op  string
	key uint64
	val int64
}
type eOut struct {
	st  string
	val int64
}
type eState struct {
	exists bool
	val    int64
}

func runEsiste(path string, timeout int) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var ops []porcupine.Operation
	counts := map[string]int{}
	for sc.Scan() {
		p := strings.Split(sc.Text(), ",")
		k, _ := strconv.ParseUint(p[1], 10, 64)
		v, _ := strconv.ParseInt(p[2], 10, 64)
		t0, _ := strconv.ParseInt(p[3], 10, 64)
		t1, _ := strconv.ParseInt(p[4], 10, 64)
		if p[5] == "unknown" {
			t1 = 1 << 62
		}
		counts[p[0]+"/"+p[5]]++
		ops = append(ops, porcupine.Operation{ClientId: 0, Input: eIn{p[0], k, v}, Call: t0, Output: eOut{p[5], v}, Return: t1})
	}
	model := porcupine.Model{
		Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
			m := map[uint64][]porcupine.Operation{}
			for _, o := range h {
				k := o.Input.(eIn).key
				m[k] = append(m[k], o)
			}
			var r [][]porcupine.Operation
			for _, v := range m {
				r = append(r, v)
			}
			return r
		},
		Init: func() interface{} { return eState{false, 0} },
		Step: func(st, i, o interface{}) (bool, interface{}) {
			s := st.(eState)
			in := i.(eIn)
			out := o.(eOut)
			switch in.op {
			case "put":
				return true, eState{true, in.val}
			case "del":
				return true, eState{false, 0}
			case "inc":
				switch out.st {
				case "ok":
					if !s.exists {
						return false, s
					}
					return true, eState{true, s.val + 1}
				case "nomatch":
					return !s.exists, s
				default: // ignoto: con o senza effetto, come se fosse avvenuto qui
					if s.exists {
						return true, eState{true, s.val + 1}
					}
					return true, s
				}
			case "read":
				if out.st == "absent" {
					return !s.exists, s
				}
				return s.exists && s.val == out.val, s
			}
			return false, s
		},
		Equal: func(a, b interface{}) bool { return a.(eState) == b.(eState) },
	}
	start := time.Now()
	res := porcupine.CheckOperationsTimeout(model, ops, time.Duration(timeout)*time.Second)
	fmt.Printf("{\"check\": \"porcupine-esiste\", \"ops\": %d, \"counts\": \"%v\", \"result\": \"%v\", \"seconds\": %.1f}\n", len(ops), counts, res, time.Since(start).Seconds())
	if res == porcupine.Illegal {
		os.Exit(1)
	}
}
