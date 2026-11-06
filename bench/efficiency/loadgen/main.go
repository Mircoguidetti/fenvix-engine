// Load generator of the E1 efficiency study (bench/efficiency, see README.md there).
//
// It reproduces, against any MongoDB-compatible server, the write load of the M30 peak of the
// Ireland round (code-irlanda, irlanda/programmi/carica + prova_infinite.py `carica`): the same
// three collections (messaggi 50%, eventi 35%, ordini 15%), the same documents (fields, sizes,
// zipf text from the same vocabulary, exported to vocab.json), the same indexes (cliente_ts on the
// three collections, stato_ts on ordini), unordered insertMany of 1,000 documents split by
// collection, N databases with W workers each, no explicit write concern (server default).
// The read mode is the hot single-document read of the same program (Q2: an order by _id, limit 1).
//
// For each 5-second window it prints one JSON line with the documents (or reads) done, the CPU
// seconds of the server process (--pid, utime+stime of all its threads from /proc) and of this
// generator, so CPU per operation can be computed for any steady part of the run.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

//go:embed vocab.json
var vocabJSON []byte

var (
	uri        = flag.String("uri", "mongodb://127.0.0.1:27017/?directConnection=true", "connection string")
	mode       = flag.String("mode", "write", "write | read")
	nDBs       = flag.Int("dbs", 6, "databases (app_data_1..N)")
	workers    = flag.Int("workers", 16, "write: workers per database; read: workers in total")
	batch      = flag.Int("batch", 1000, "documents per batch (split by collection, as prova_infinite.py)")
	gb         = flag.Float64("gb", 6.6, "logical GB per database the client schedule is built for (clients = gb*1024/mb)")
	mbClient   = flag.Float64("mb-client", 2.0, "average client size in MB")
	reserved   = flag.Float64("reserved", 0.4883, "share of dormant clients (they write only in the first 60%)")
	duration   = flag.Int("duration", 600, "seconds to run")
	maxDocs    = flag.Int64("max-docs", 0, "write: stop after this many documents in total (0: no limit)")
	rate       = flag.Float64("rate", 0, "operations per second in total (0: as fast as possible)")
	pid        = flag.Int("pid", 0, "server process whose CPU is sampled (0: none)")
	seed       = flag.Uint64("seed", 104, "random seed")
	hotIDs     = flag.Int("hot-ids", 20000, "read: order _ids per database kept as the hot set")
	readDBs    = flag.Int("read-dbs", 0, "read: databases to read (0: all of --dbs)")
	dbPrefix   = flag.String("db-prefix", "app_data_", "database name prefix")
	noIndexes  = flag.Bool("no-indexes", false, "write: do not create the secondary indexes")
	label      = flag.String("label", "", "free text copied to every line")
	windowSecs = flag.Int("window", 5, "seconds per output line")
)

const clkTck = 100.0

// cpuSeconds returns utime+stime of a process (all its threads), in seconds.
func cpuSeconds(p int) float64 {
	path := "/proc/self/stat"
	if p > 0 {
		path = "/proc/" + strconv.Itoa(p) + "/stat"
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return -1
	}
	s := string(b)
	s = s[strings.LastIndexByte(s, ')')+2:]
	f := strings.Fields(s)
	u, _ := strconv.ParseFloat(f[11], 64)
	st, _ := strconv.ParseFloat(f[12], 64)
	return (u + st) / clkTck
}

// ---------------------------------------------------------------- documents (port of prova_infinite.py)

var (
	vocab    []string
	cumVocab []float64
	ruoli    = []string{"user", "assistant", "tool"}
	modelli  = []string{"small-2026", "medium-2026", "large-2026"}
	tipiEv   = []string{"lettura", "stato", "allarme", "batteria", "posizione", "avvio"}
	stati    = []string{"nuovo", "pagato", "spedito", "consegnato", "reso"}
	citta    = []string{"Milano", "Roma", "Torino", "Napoli", "Bologna", "Firenze", "Berlin", "Paris", "Madrid", "Lyon"}
	tStart   = time.Date(2025, 11, 1, 0, 0, 0, 0, time.UTC)
	tSpanSec = 365.0 * 86400
)

const endDormant = 0.6

func initVocab() {
	if err := json.Unmarshal(vocabJSON, &vocab); err != nil {
		panic(err)
	}
	acc := 0.0
	for k := range vocab {
		acc += 1.0 / math.Pow(float64(k+1), 0.9)
		cumVocab = append(cumVocab, acc)
	}
}

func pick(r *rand.Rand, cum []float64) int {
	x := r.Float64() * cum[len(cum)-1]
	i := sort.SearchFloat64s(cum, x)
	if i >= len(cum) {
		i = len(cum) - 1
	}
	return i
}

// text: words with zipf frequency and sometimes a number, cut to n bytes (testo()).
func text(r *rand.Rand, n int) string {
	k := n/5 + 2
	words := make([]string, k)
	for i := range words {
		words[i] = vocab[pick(r, cumVocab)]
	}
	if r.Float64() < 0.5 {
		words[r.IntN(k)] = strconv.Itoa(r.IntN(1_000_000))
	}
	s := strings.Join(words, " ")
	if len(s) > n {
		s = s[:n]
	}
	return s
}

func rangeInt(r *rand.Rand, lo, hi int) int { return lo + r.IntN(hi-lo) } // [lo, hi), as randrange
func round2(x float64) float64             { return math.Round(x*100) / 100 }

func docMessaggio(r *rand.Rand, cli string, ts time.Time) bson.D {
	return bson.D{{"cliente", cli}, {"ts", ts}, {"conv", fmt.Sprintf("conv-%d", r.IntN(40))},
		{"ruolo", ruoli[r.IntN(3)]}, {"modello", modelli[r.IntN(3)]}, {"token", rangeInt(r, 20, 2000)},
		{"testo", text(r, rangeInt(r, 60, 340))}}
}

func docEvento(r *rand.Rand, cli string, ts time.Time) bson.D {
	return bson.D{{"cliente", cli}, {"ts", ts}, {"disp", fmt.Sprintf("dev-%d", r.IntN(8))},
		{"tipo", tipiEv[r.IntN(6)]}, {"fw", "1.4.2"},
		{"valori", bson.D{{"temp", round2(15 + 25*r.Float64())}, {"batt", r.IntN(101)},
			{"rssi", -rangeInt(r, 30, 95)}, {"ok", r.Float64() < 0.97}}},
		{"nota", text(r, rangeInt(r, 10, 120))}}
}

func docOrdine(r *rand.Rand, cli string, ts time.Time) bson.D {
	n := rangeInt(r, 1, 5)
	righe := bson.A{}
	tot := 0.0
	for i := 0; i < n; i++ {
		q := rangeInt(r, 1, 4)
		p := round2(3 + 247*r.Float64())
		tot += p * float64(q)
		righe = append(righe, bson.D{{"sku", fmt.Sprintf("SKU-%05d", r.IntN(5000))}, {"q", q}, {"prezzo", p},
			{"desc", text(r, rangeInt(r, 20, 60))}})
	}
	return bson.D{{"cliente", cli}, {"ts", ts}, {"stato", stati[r.IntN(5)]}, {"righe", righe},
		{"totale", round2(tot)}, {"valuta", "EUR"},
		{"indirizzo", bson.D{{"via", text(r, 24)}, {"citta", citta[r.IntN(len(citta))]},
			{"cap", strconv.Itoa(rangeInt(r, 10000, 99999))}}},
		{"note", text(r, rangeInt(r, 40, 200))}}
}

// schedule: the clients of one database and their weights in the two phases (prepara_clienti, cmd_carica).
type schedule struct {
	names      []string
	cum1, cum2 []float64
	active2    []int
	target     float64 // logical bytes of the database the schedule is built for
}

func newSchedule(r *rand.Rand) *schedule {
	n := int(*gb * 1024 / *mbClient)
	if n < 50 {
		n = 50
	}
	sizes := make([]float64, n)
	for i := range sizes {
		x := math.Exp(r.NormFloat64() * 0.5)
		sizes[i] = math.Min(4.0, math.Max(0.25, x)) * *mbClient
	}
	idx := r.Perm(n)
	res := map[int]bool{}
	for _, i := range idx[:int(float64(n)**reserved)] {
		res[i] = true
	}
	s := &schedule{target: *gb * (1 << 30)}
	acc1, acc2 := 0.0, 0.0
	for i := 0; i < n; i++ {
		s.names = append(s.names, fmt.Sprintf("cli-%06d", i))
		w := sizes[i]
		if res[i] {
			w /= endDormant
		} else {
			acc2 += sizes[i]
			s.cum2 = append(s.cum2, acc2)
			s.active2 = append(s.active2, i)
		}
		acc1 += w
		s.cum1 = append(s.cum1, acc1)
	}
	return s
}

// ---------------------------------------------------------------- rate limiter shared by all workers

type limiter struct {
	mu   sync.Mutex
	next time.Time
	step time.Duration
}

func (l *limiter) take(n int) {
	if l == nil {
		return
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	at := l.next
	l.next = l.next.Add(time.Duration(n) * l.step)
	l.mu.Unlock()
	if d := time.Until(at); d > 0 {
		time.Sleep(d)
	}
}

// ---------------------------------------------------------------- main

type window struct {
	Label     string  `json:"label"`
	Mode      string  `json:"mode"`
	T         float64 `json:"t"`
	Secs      float64 `json:"secs"`
	Ops       int64   `json:"ops"`
	TotalOps  int64   `json:"total_ops"`
	Bytes     int64   `json:"bytes,omitempty"`
	OpsPerSec float64 `json:"ops_per_sec"`
	SrvCPU    float64 `json:"server_cpu_s"`
	SrvCores  float64 `json:"server_cores"`
	SrvUsOp   float64 `json:"server_us_per_op"`
	GenCores  float64 `json:"gen_cores"`
	Errors    int64   `json:"errors"`
	P50ms     float64 `json:"p50_ms,omitempty"`
	P99ms     float64 `json:"p99_ms,omitempty"`
}

func main() {
	flag.Parse()
	initVocab()
	ctx := context.Background()
	cl, err := mongo.Connect(options.Client().ApplyURI(*uri).SetMaxPoolSize(uint64(*workers**nDBs + 8)).
		SetCompressors([]string{}))
	if err != nil {
		panic(err)
	}
	defer cl.Disconnect(ctx)
	var lim *limiter
	if *rate > 0 {
		lim = &limiter{step: time.Duration(float64(time.Second) / *rate)}
	}
	var ops, byteCount, errs atomic.Int64
	var latMu sync.Mutex
	var lats []float64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	switch *mode {
	case "write":
		for d := 1; d <= *nDBs; d++ {
			db := cl.Database(fmt.Sprintf("%s%d", *dbPrefix, d))
			if !*noIndexes {
				for _, c := range []string{"messaggi", "eventi", "ordini"} {
					_, err := db.Collection(c).Indexes().CreateOne(ctx, mongo.IndexModel{
						Keys: bson.D{{"cliente", 1}, {"ts", -1}}, Options: options.Index().SetName("cliente_ts")})
					if err != nil {
						panic(err)
					}
				}
				if _, err := db.Collection("ordini").Indexes().CreateOne(ctx, mongo.IndexModel{
					Keys: bson.D{{"stato", 1}, {"ts", -1}}, Options: options.Index().SetName("stato_ts")}); err != nil {
					panic(err)
				}
			}
			sr := rand.New(rand.NewPCG(*seed, uint64(d)))
			sch := newSchedule(sr)
			var dbBytes atomic.Int64
			for w := 0; w < *workers; w++ {
				wg.Add(1)
				go func(d, w int) {
					defer wg.Done()
					r := rand.New(rand.NewPCG(*seed*1000+uint64(d), uint64(w)))
					colls := map[string]*mongo.Collection{"messaggi": db.Collection("messaggi"),
						"eventi": db.Collection("eventi"), "ordini": db.Collection("ordini")}
					for {
						select {
						case <-stop:
							return
						default:
						}
						if *maxDocs > 0 && ops.Load() >= *maxDocs {
							return
						}
						p := math.Min(float64(dbBytes.Load())/sch.target, 0.9999)
						baseTS := float64(tStart.Unix()) + p*tSpanSec
						per := map[string][]any{}
						nb := 0
						for i := 0; i < *batch; i++ {
							var ci int
							if p < endDormant {
								ci = pick(r, sch.cum1)
							} else {
								ci = sch.active2[pick(r, sch.cum2)]
							}
							x := r.Float64()
							coll := "ordini"
							if x < 0.5 {
								coll = "messaggi"
							} else if x < 0.85 {
								coll = "eventi"
							}
							sec := baseTS + 600*r.Float64()
							ts := time.Unix(int64(sec), int64((sec-math.Floor(sec))*1e9)).UTC().Truncate(time.Millisecond)
							var doc bson.D
							switch coll {
							case "messaggi":
								doc = docMessaggio(r, sch.names[ci], ts)
							case "eventi":
								doc = docEvento(r, sch.names[ci], ts)
							default:
								doc = docOrdine(r, sch.names[ci], ts)
							}
							doc = append(doc, bson.E{Key: "_id", Value: bson.NewObjectID()})
							raw, _ := bson.Marshal(doc)
							nb += len(raw)
							per[coll] = append(per[coll], bson.Raw(raw))
						}
						lim.take(*batch)
						for _, coll := range []string{"messaggi", "eventi", "ordini"} {
							docs := per[coll]
							if len(docs) == 0 {
								continue
							}
							t0 := time.Now()
							_, err := colls[coll].InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
							el := float64(time.Since(t0).Microseconds()) / 1000
							if err != nil {
								errs.Add(1)
								fmt.Fprintln(os.Stderr, "insert error:", err)
								time.Sleep(time.Second)
								continue
							}
							ops.Add(int64(len(docs)))
							latMu.Lock()
							lats = append(lats, el)
							latMu.Unlock()
						}
						dbBytes.Add(int64(nb))
						byteCount.Add(int64(nb))
					}
				}(d, w)
			}
		}
	case "read":
		nr := *readDBs
		if nr == 0 {
			nr = *nDBs
		}
		type target struct {
			coll *mongo.Collection
			ids  []bson.ObjectID
		}
		var targets []target
		for d := 1; d <= nr; d++ {
			c := cl.Database(fmt.Sprintf("%s%d", *dbPrefix, d)).Collection("ordini")
			cur, err := c.Find(ctx, bson.D{}, options.Find().SetProjection(bson.D{{"_id", 1}}).SetLimit(int64(*hotIDs)))
			if err != nil {
				panic(err)
			}
			var ids []bson.ObjectID
			for cur.Next(ctx) {
				var x struct {
					ID bson.ObjectID `bson:"_id"`
				}
				if err := cur.Decode(&x); err == nil {
					ids = append(ids, x.ID)
				}
			}
			cur.Close(ctx)
			if len(ids) == 0 {
				panic("no orders in " + c.Database().Name())
			}
			targets = append(targets, target{c, ids})
			fmt.Fprintf(os.Stderr, "%s: %d hot ids\n", c.Database().Name(), len(ids))
		}
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				r := rand.New(rand.NewPCG(*seed, uint64(w)))
				var local []float64
				for {
					select {
					case <-stop:
						latMu.Lock()
						lats = append(lats, local...)
						latMu.Unlock()
						return
					default:
					}
					tg := targets[r.IntN(len(targets))]
					id := tg.ids[r.IntN(len(tg.ids))]
					lim.take(1)
					t0 := time.Now()
					var out bson.Raw
					err := tg.coll.FindOne(ctx, bson.D{{"_id", id}}).Decode(&out)
					el := float64(time.Since(t0).Microseconds()) / 1000
					if err != nil {
						errs.Add(1)
						continue
					}
					ops.Add(1)
					local = append(local, el)
					if len(local) >= 256 {
						latMu.Lock()
						lats = append(lats, local...)
						latMu.Unlock()
						local = local[:0]
					}
				}
			}(w)
		}
	default:
		panic("mode")
	}

	enc := json.NewEncoder(os.Stdout)
	t0 := time.Now()
	lastT, lastOps, lastBytes := t0, int64(0), int64(0)
	lastSrv, lastGen := cpuSeconds(*pid), cpuSeconds(0)
	tick := time.NewTicker(time.Duration(*windowSecs) * time.Second)
	end := time.After(time.Duration(*duration) * time.Second)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	emit := func() {
		now := time.Now()
		o, b := ops.Load(), byteCount.Load()
		srv, gen := cpuSeconds(*pid), cpuSeconds(0)
		secs := now.Sub(lastT).Seconds()
		w := window{Label: *label, Mode: *mode, T: now.Sub(t0).Seconds(), Secs: secs, Ops: o - lastOps, TotalOps: o,
			Bytes: b - lastBytes, OpsPerSec: float64(o-lastOps) / secs, SrvCPU: srv - lastSrv,
			SrvCores: (srv - lastSrv) / secs, GenCores: (gen - lastGen) / secs, Errors: errs.Load()}
		if o > lastOps && *pid > 0 {
			w.SrvUsOp = (srv - lastSrv) * 1e6 / float64(o-lastOps)
		}
		latMu.Lock()
		if len(lats) > 0 {
			sort.Float64s(lats)
			w.P50ms = lats[len(lats)/2]
			w.P99ms = lats[len(lats)*99/100]
			lats = lats[:0]
		}
		latMu.Unlock()
		enc.Encode(w)
		lastT, lastOps, lastBytes, lastSrv, lastGen = now, o, b, srv, gen
	}
loop:
	for {
		select {
		case <-tick.C:
			emit()
		case <-end:
			break loop
		case <-done:
			break loop
		}
	}
	close(stop)
	wg.Wait()
	emit()
}
