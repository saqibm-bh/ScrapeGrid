"""Simulation engine for the ScrapeGrid dashboard.

Exposes:
    create_engine, get_engine, SimulationEngine, SingleNodeScraper,
    DistributedWorker, DistributedFleet, GossipProtocol, and run_mapreduce.

Design notes:
    Everything runs in local threads with fake URLs and fake page content. It
    demonstrates distributed-systems behavior without making real web requests.
"""



import math
import time
import random
import threading
import collections
from algorithms.bloom_filter import BloomFilter
from algorithms.circuit_breaker import CircuitBreaker
from algorithms.clocks import LamportClock, VectorClock
from algorithms.consistent_hash import ConsistentHashRing
from algorithms.raft import RaftNode
from algorithms.token_bucket import TokenBucket
from utils.logger import EventLog
from utils.url_generator import DOMAIN_CATEGORIES, fake_page_content, generate_url_pool

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL SINGLETON
# Streamlit reruns the whole script on every refresh, so we hold the engine
# at module level so it survives between reruns.
# ─────────────────────────────────────────────────────────────────────────────
_engine_instance: "SimulationEngine | None" = None


def create_engine(config: dict) -> "SimulationEngine":
    global _engine_instance
    _engine_instance = SimulationEngine(config)
    return _engine_instance


def get_engine() -> "SimulationEngine | None":
    return _engine_instance


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _ip_block_fires(request_count: int, lam: float) -> bool:
    """P(block) = 1 - e^(-λ * request_count). High-frequency workers hit this fast."""
    prob = 1.0 - math.exp(-lam * request_count)
    return random.random() < prob


def _domain_of(url: str) -> str:
    return url.split("/")[2]


def _category_of(url: str, domain_map: dict) -> str:
    return domain_map.get(url, "general")


# ─────────────────────────────────────────────────────────────────────────────
# GOSSIP PROTOCOL
# ─────────────────────────────────────────────────────────────────────────────

class GossipProtocol:
    """
    Epidemic membership protocol.  Each 'round' two random nodes exchange
    their view of the cluster.  Converges in O(log N) rounds.
    No central polling — no single point of failure in health monitoring.
    """

    def __init__(self):
        self._lock             = threading.Lock()
        self.rounds            = 0
        self.known_nodes: dict = {}       # {node_id: {status, urls, lamport}}
        self.recent_exchanges  = []       # last 20 gossip events

    def update_node(self, node_id: int, info: dict) -> None:
        with self._lock:
            self.known_nodes[node_id] = info

    def gossip_round(self, node_ids: list) -> None:
        if len(node_ids) < 2:
            return
        a, b = random.sample(node_ids, 2)
        with self._lock:
            self.rounds += 1
            self.recent_exchanges.append({
                "round":        self.rounds,
                "from":         a,
                "to":           b,
                "nodes_shared": len(self.known_nodes),
            })
            if len(self.recent_exchanges) > 20:
                self.recent_exchanges.pop(0)

    def state(self, total_expected: int) -> dict:
        with self._lock:
            known      = len(self.known_nodes)
            convergence = min(1.0, known / max(total_expected, 1))
            return {
                "rounds":           self.rounds,
                "known_nodes":      known,
                "convergence":      round(convergence, 3),
                "recent_exchanges": list(self.recent_exchanges[-5:]),
            }


# ─────────────────────────────────────────────────────────────────────────────
# MAP-REDUCE PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def run_mapreduce(scraped_results: list) -> dict:
    """
    Map:    each result → (category, quality_score, word_count)
    Shuffle: group by category key
    Reduce: compute aggregate stats per category
    """
    mapped: dict = collections.defaultdict(list)
    for r in scraped_results:
        mapped[r.get("category", "general")].append(r)

    reduced = {}
    for cat, items in mapped.items():
        qualities = [i.get("quality_score", 0) for i in items]
        words     = [i.get("word_count", 0)    for i in items]
        reduced[cat] = {
            "item_count":  len(items),
            "avg_quality": round(sum(qualities) / max(len(qualities), 1), 3),
            "total_words": sum(words),
            "avg_words":   round(sum(words) / max(len(words), 1), 1),
        }
    return reduced


# ─────────────────────────────────────────────────────────────────────────────
# EVENT LOG  (Lamport-ordered)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# SINGLE NODE SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

class SingleNodeScraper:
    """
    One worker processing URLs sequentially.

    Failure story:
      • Block fires → log event, enter exponential backoff, switch domain
      • All domains eventually heat up → throughput degrades gracefully
      • dead_time_s / elapsed = dead time percentage shown on dashboard
    """

    def __init__(self, url_pool: list, domain_map: dict, lam: float, speed: float):
        self.domain_map = domain_map
        self.lam        = lam
        self.speed      = speed

        # Per-category deques — we cycle through categories round-robin
        self._queues: dict = collections.defaultdict(collections.deque)
        for url in url_pool:
            self._queues[_category_of(url, domain_map)].append(url)

        self._domain_req: dict = collections.defaultdict(int)   # domain → request count

        # Public stats (read by snapshot())
        self.urls_scraped       = 0
        self.blocks             = 0
        self.block_events       = []
        self.status             = "idle"
        self.current_domain     = None
        self.dead_time_s        = 0.0
        self.throughput_history = []   # [{t, urls}]

        self._start_time = 0.0
        self._running    = False
        self._lock       = threading.Lock()

    def start(self, start_time: float) -> None:
        self._start_time = start_time
        self._running    = True
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self) -> None:
        self._running = False

    def _elapsed(self) -> float:
        return time.time() - self._start_time

    def _run(self) -> None:
        categories    = list(self._queues.keys())
        cat_idx       = 0
        last_snapshot = 0.0

        while self._running:
            now = self._elapsed()

            # Throughput snapshot every second
            if now - last_snapshot >= 1.0:
                with self._lock:
                    self.throughput_history.append({"t": round(now, 1), "urls": self.urls_scraped})
                last_snapshot = now

            # Find a non-empty category (round-robin)
            found = False
            for _ in range(len(categories)):
                cat = categories[cat_idx % len(categories)]
                cat_idx += 1
                if self._queues[cat]:
                    found = True
                    break

            if not found:
                self.status = "done"
                break

            url    = self._queues[cat].popleft()
            domain = _domain_of(url)

            with self._lock:
                self.status         = f"scraping:{cat}"
                self.current_domain = cat

            # Simulated latency
            page  = fake_page_content(url)
            sleep = (page["simulated_latency_ms"] / 1000.0) / self.speed
            time.sleep(min(sleep, 0.3))

            req_count = self._domain_req[domain]

            if _ip_block_fires(req_count, self.lam):
                # ── BLOCKED ──────────────────────────────────────────────────
                backoff = (2 ** min(self.blocks, 5)) * 0.5 / self.speed
                ev = {
                    "time":                 round(self._elapsed(), 1),
                    "domain":               domain,
                    "category":             cat,
                    "requests_before_block": req_count,
                    "backoff_s":            round(backoff, 2),
                }
                with self._lock:
                    self.blocks += 1
                    self.block_events.append(ev)
                    self.status = f"blocked:{cat}"
                    # Reset domain counter so it can recover
                    self._domain_req[domain] = 0

                # Put URL back so it isn't lost
                self._queues[cat].appendleft(url)

                t0 = time.time()
                time.sleep(backoff)
                with self._lock:
                    self.dead_time_s += time.time() - t0
            else:
                # ── SUCCESS ───────────────────────────────────────────────────
                with self._lock:
                    self._domain_req[domain] += 1
                    self.urls_scraped += 1

    def snapshot(self, elapsed: float) -> dict:
        with self._lock:
            dead_pct = round(self.dead_time_s / max(elapsed, 0.001) * 100, 1)
            hist     = self.throughput_history
            if len(hist) >= 2:
                dt   = max(hist[-1]["t"] - hist[-2]["t"], 0.001)
                rate = round((hist[-1]["urls"] - hist[-2]["urls"]) / dt, 1)
            else:
                rate = 0.0
            return {
                "urls_scraped":       self.urls_scraped,
                "blocks":             self.blocks,
                "status":             self.status,
                "current_domain":     self.current_domain,
                "dead_pct":           dead_pct,
                "rate":               rate,
                "block_events":       list(self.block_events),
                "throughput_history": list(hist),
            }


# ─────────────────────────────────────────────────────────────────────────────
# DISTRIBUTED WORKER
# ─────────────────────────────────────────────────────────────────────────────

class DistributedWorker:
    """
    One worker in the fleet.
    Uses CircuitBreaker for IP-block state, VectorClock for causality.
    Pulls from own shard deque; steals from back of busiest shard when idle.
    Must acquire a TokenBucket token before each scrape (rate limiting).
    """

    def __init__(
        self,
        node_id:       int,
        shard_name:    str,
        shard_deque:   collections.deque,
        all_shards:    dict,
        domain_map:    dict,
        lam:           float,
        speed:         float,
        global_clock:  LamportClock,
        token_buckets: dict,
        result_store:  list,
        result_lock:   threading.Lock,
        event_log:     EventLog,
        gossip:        GossipProtocol,
        num_nodes:     int,
    ):
        self.node_id     = node_id
        self.shard_name  = shard_name
        self._deque      = shard_deque
        self._all_shards = all_shards
        self.domain_map  = domain_map
        self.lam         = lam
        self.speed       = speed
        self._clock      = global_clock
        self._buckets    = token_buckets
        self._results    = result_store
        self._res_lock   = result_lock
        self._events     = event_log
        self._gossip     = gossip

        self.cb = CircuitBreaker(failure_threshold=1)
        self.vc = VectorClock(node_id % max(num_nodes, 1), num_nodes)

        # Per-domain request counter (drives block probability)
        self._domain_req: dict = collections.defaultdict(int)

        # Public stats
        self.urls_scraped   = 0
        self.blocks         = 0
        self.tasks_stolen   = 0
        self.status         = "idle"
        self.current_domain = None

        self._start_time = 0.0
        self._running    = False
        self._killed     = False
        self._lock       = threading.Lock()

    def start(self, start_time: float) -> None:
        self._start_time = start_time
        self._running    = True
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self) -> None:
        self._running = False

    def kill(self) -> None:
        self._killed  = True
        self._running = False
        with self._lock:
            self.status = "dead"

    def _elapsed(self) -> float:
        return time.time() - self._start_time

    def _steal(self) -> "str | None":
        """Work stealing: grab from the back of the busiest other shard."""
        best     = None
        best_len = 0
        for name, dq in self._all_shards.items():
            if name != self.shard_name and len(dq) > best_len:
                best, best_len = (name, dq), len(dq)
        if best and best_len > 0:
            try:
                url = best[1].pop()   # steal from back
                with self._lock:
                    self.tasks_stolen += 1
                self._events.log(
                    source  = f"Node-{self.node_id}",
                    level   = "info",
                    message = f"Stole task from shard '{best[0]}' (depth was {best_len})",
                    lamport = self._clock.increment(),
                    t       = self._elapsed(),
                )
                return url
            except IndexError:
                pass
        return None

    def _run(self) -> None:
        while self._running and not self._killed:
            # ── Circuit breaker gate ────────────────────────────────────────
            if not self.cb.allow_request():
                with self._lock:
                    self.status = f"cooling:{self.shard_name}"
                time.sleep(0.05)
                continue

            # ── Get next URL (own deque or steal) ───────────────────────────
            url = None
            if self._deque:
                try:
                    url = self._deque.popleft()
                except IndexError:
                    pass
            if url is None:
                url = self._steal()
            if url is None:
                with self._lock:
                    self.status = "idle"
                time.sleep(0.05)
                continue

            cat    = _category_of(url, self.domain_map)
            domain = _domain_of(url)

            # ── Token bucket check ──────────────────────────────────────────
            bucket = self._buckets.get(cat)
            if bucket and not bucket.consume():
                self._deque.appendleft(url)   # rate limited — requeue
                time.sleep(0.02 / self.speed)
                continue

            with self._lock:
                self.status         = f"scraping:{cat}"
                self.current_domain = cat

            # ── Simulated latency ───────────────────────────────────────────
            page  = fake_page_content(url)
            sleep = (page["simulated_latency_ms"] / 1000.0) / self.speed
            time.sleep(min(sleep, 0.2))

            req_count = self._domain_req[domain]

            if _ip_block_fires(req_count, self.lam):
                # ── BLOCKED ─────────────────────────────────────────────────
                self.cb.record_failure()
                with self._lock:
                    self.blocks += 1
                    self.status = f"blocked:{cat}"
                    self._domain_req[domain] = 0   # reset so recovery is possible

                self._events.log(
                    source  = f"Node-{self.node_id}",
                    level   = "error",
                    message = f"IP BLOCKED on '{cat}' ({domain}) after {req_count} reqs. "
                              f"Circuit={self.cb.state}, cooldown={self.cb.cooldown_remaining:.1f}s",
                    lamport = self._clock.increment(),
                    t       = self._elapsed(),
                )
                # Return URL to queue — other workers or this node after cooldown will handle it
                self._deque.appendleft(url)

            else:
                # ── SUCCESS ──────────────────────────────────────────────────
                self.cb.record_success()
                with self._lock:
                    self._domain_req[domain] += 1
                    self.urls_scraped += 1

                vec = self.vc.tick()
                lts = self._clock.increment()

                result = {
                    **page,
                    "category": cat,
                    "node_id":  self.node_id,
                    "lamport":  lts,
                    "vector":   vec,
                }
                with self._res_lock:
                    self._results.append(result)

                self._gossip.update_node(self.node_id, {
                    "status":  "scraping",
                    "urls":    self.urls_scraped,
                    "lamport": lts,
                })

        with self._lock:
            if not self._killed:
                self.status = "done"

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "status":             self.status,
                "circuit_state":      self.cb.state,
                "urls_scraped":       self.urls_scraped,
                "blocks":             self.blocks,
                "domain":             self.current_domain,
                "tasks_stolen":       self.tasks_stolen,
                "cooldown_remaining": self.cb.cooldown_remaining,
            }


# ─────────────────────────────────────────────────────────────────────────────
# DISTRIBUTED FLEET  (coordinator of workers)
# ─────────────────────────────────────────────────────────────────────────────

class DistributedFleet:
    """
    Manages the pool of DistributedWorkers.
    Owns per-domain shard deques, TokenBuckets, auto-scaling, and aggregated stats.
    """

    def __init__(
        self,
        url_pool:      list,
        domain_map:    dict,
        lam:           float,
        speed:         float,
        initial_nodes: int,
        max_nodes:     int,
        min_nodes:     int,
        auto_scale:    bool,
        global_clock:  LamportClock,
        event_log:     EventLog,
        gossip:        GossipProtocol,
        hash_ring:     ConsistentHashRing,
    ):
        self.domain_map  = domain_map
        self.lam         = lam
        self.speed       = speed
        self.max_nodes   = max_nodes
        self.min_nodes   = min_nodes
        self.auto_scale  = auto_scale
        self._clock      = global_clock
        self._events     = event_log
        self._gossip     = gossip
        self._ring       = hash_ring
        self._start_time = 0.0
        self._running    = False

        # One deque per domain category (shard)
        cats = list(DOMAIN_CATEGORIES.keys())
        self._shards: dict = {cat: collections.deque() for cat in cats}
        for url in url_pool:
            cat = _category_of(url, domain_map)
            if cat in self._shards:
                self._shards[cat].append(url)
            else:
                self._shards[cats[0]].append(url)

        # Per-category token buckets
        self._buckets: dict = {cat: TokenBucket(rate=15.0, capacity=30.0) for cat in cats}

        # Shared result store
        self._results    = []
        self._res_lock   = threading.Lock()

        # Workers
        self._workers: dict    = {}   # {node_id: DistributedWorker}
        self._worker_lock      = threading.Lock()
        self._next_id          = 0

        # Stats
        self.scale_events       = []
        self.block_events       = []    # fleet-level [{time, category}]
        self.throughput_history = []

        # Spawn initial workers
        for _ in range(initial_nodes):
            self._spawn_worker()

    # ── Worker lifecycle ──────────────────────────────────────────────────────

    def _shard_for(self, node_id: int) -> str:
        """Use consistent hash ring to assign worker to a shard."""
        name = self._ring.get_node(str(node_id))
        # Fallback: if ring returns a name not in shards, pick by index
        if name not in self._shards:
            cats = list(self._shards.keys())
            name = cats[node_id % len(cats)]
        return name

    def _spawn_worker(self) -> int:
        nid        = self._next_id
        self._next_id += 1
        shard_name = self._shard_for(nid)

        w = DistributedWorker(
            node_id      = nid,
            shard_name   = shard_name,
            shard_deque  = self._shards[shard_name],
            all_shards   = self._shards,
            domain_map   = self.domain_map,
            lam          = self.lam,
            speed        = self.speed,
            global_clock = self._clock,
            token_buckets= self._buckets,
            result_store = self._results,
            result_lock  = self._res_lock,
            event_log    = self._events,
            gossip       = self._gossip,
            num_nodes    = self.max_nodes,
        )

        with self._worker_lock:
            self._workers[nid] = w

        if self._running:
            w.start(self._start_time)

        self._events.log(
            source  = "Fleet",
            level   = "success",
            message = f"Spawned Node-{nid} → shard '{shard_name}'",
            lamport = self._clock.increment(),
            t       = self._elapsed(),
        )
        return nid

    def _kill_worker(self, nid: int) -> None:
        with self._worker_lock:
            w = self._workers.get(nid)
        if w:
            w.kill()
            self._events.log(
                source  = "Fleet",
                level   = "warning",
                message = f"Killed Node-{nid} (scale-down / crash)",
                lamport = self._clock.increment(),
                t       = self._elapsed(),
            )

    def _elapsed(self) -> float:
        return time.time() - self._start_time

    # ── Start / stop ──────────────────────────────────────────────────────────

    def start(self, start_time: float) -> None:
        self._start_time = start_time
        self._running    = True

        with self._worker_lock:
            for w in self._workers.values():
                w.start(start_time)

        threading.Thread(target=self._autoscale_loop,   daemon=True).start()
        threading.Thread(target=self._snapshot_loop,    daemon=True).start()

    def stop(self) -> None:
        self._running = False
        with self._worker_lock:
            for w in self._workers.values():
                w.stop()

    # ── Background loops ──────────────────────────────────────────────────────

    def _snapshot_loop(self) -> None:
        """Record cumulative throughput and fleet-level block events every second."""
        while self._running:
            now   = self._elapsed()
            total = sum(w.urls_scraped for w in self._workers.values())
            self.throughput_history.append({"t": round(now, 1), "urls": total})

            # Collect block events from workers (status string is the signal)
            with self._worker_lock:
                for w in self._workers.values():
                    if "blocked" in w.status:
                        cat = w.current_domain or "?"
                        ev  = {"time": round(now, 1), "category": cat}
                        # Deduplicate: only append if new
                        if not self.block_events or \
                           self.block_events[-1]["time"] != ev["time"] or \
                           self.block_events[-1]["category"] != cat:
                            self.block_events.append(ev)

            time.sleep(1.0)

    def _autoscale_loop(self) -> None:
        """
        Every 10 seconds:
          Scale UP  when queue is deep AND block rate is high  (demand > capacity)
          Scale DOWN when queue is shallow AND workers are idle (capacity > demand)
        """
        if not self.auto_scale:
            return

        while self._running:
            time.sleep(10.0)
            if not self._running:
                break

            with self._worker_lock:
                alive = [w for w in self._workers.values() if not w._killed]

            n           = len(alive)
            queue_depth = sum(len(dq) for dq in self._shards.values())
            blocked_n   = sum(1 for w in alive if "blocked" in w.status or w.cb.state == "OPEN")
            block_rate  = blocked_n / max(n, 1)

            if queue_depth > 300 and block_rate > 0.4 and n < self.max_nodes:
                # ── SCALE UP ─────────────────────────────────────────────────
                self._spawn_worker()
                self.scale_events.append({
                    "time":      round(self._elapsed(), 1),
                    "direction": "⬆️ UP",
                    "from_n":    n,
                    "to_n":      n + 1,
                    "reason":    f"queue={queue_depth}, blocks={block_rate:.0%}",
                })
                self._events.log(
                    source  = "Scaler",
                    level   = "success",
                    message = f"SCALE UP {n}→{n+1}: queue={queue_depth}, block_rate={block_rate:.0%}",
                    lamport = self._clock.increment(),
                    t       = self._elapsed(),
                )

            elif queue_depth < 80 and n > self.min_nodes:
                # ── SCALE DOWN ───────────────────────────────────────────────
                idle = [w for w in alive if w.status == "idle"]
                if idle:
                    self._kill_worker(idle[0].node_id)
                    self.scale_events.append({
                        "time":      round(self._elapsed(), 1),
                        "direction": "⬇️ DOWN",
                        "from_n":    n,
                        "to_n":      n - 1,
                        "reason":    f"queue={queue_depth} (low load)",
                    })
                    self._events.log(
                        source  = "Scaler",
                        level   = "warning",
                        message = f"SCALE DOWN {n}→{n-1}: queue low ({queue_depth})",
                        lamport = self._clock.increment(),
                        t       = self._elapsed(),
                    )

    # ── Snapshot helpers ──────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        with self._worker_lock:
            workers = dict(self._workers)

        nodes_snap    = {nid: w.snapshot()  for nid, w in workers.items()}
        active        = sum(1 for n in nodes_snap.values()
                            if "dead" not in n["status"] and "done" not in n["status"])
        total_scraped = sum(n["urls_scraped"]  for n in nodes_snap.values())
        total_blocks  = sum(n["blocks"]        for n in nodes_snap.values())
        total_stolen  = sum(n["tasks_stolen"]  for n in nodes_snap.values())

        hist = self.throughput_history
        if len(hist) >= 2:
            dt   = max(hist[-1]["t"] - hist[-2]["t"], 0.001)
            rate = round((hist[-1]["urls"] - hist[-2]["urls"]) / dt, 1)
        else:
            rate = 0.0

        # Vector clocks per worker
        vc_data = {nid: w.vc.snapshot() for nid, w in workers.items()}

        return {
            "urls_scraped":       total_scraped,
            "blocks":             total_blocks,
            "stolen_tasks":       total_stolen,
            "active_node_count":  active,
            "nodes":              nodes_snap,
            "rate":               rate,
            "scale_events":       list(self.scale_events),
            "block_events":       list(self.block_events[-50:]),
            "throughput_history": list(hist),
            "vector_clocks":      vc_data,
            "mapreduce_results":  None,   # filled by engine at race end
        }

    def get_results(self) -> list:
        with self._res_lock:
            return list(self._results)

    def get_token_bucket_stats(self) -> dict:
        return {
            cat: {"fill": round(tb.fill_level, 3), "consumed": tb.total_consumed}
            for cat, tb in self._buckets.items()
        }

    def get_work_queue_sizes(self) -> dict:
        return {cat: len(dq) for cat, dq in self._shards.items()}


# ─────────────────────────────────────────────────────────────────────────────
# SIMULATION ENGINE  (top-level object, one per race)
# ─────────────────────────────────────────────────────────────────────────────

class SimulationEngine:
    """
    Creates and wires every subsystem.

    Config keys (all optional — shown with defaults):
      lambda_intensity  float  0.009   IP block intensity
      initial_nodes     int    4       starting worker count
      max_nodes         int    10      auto-scale ceiling
      min_nodes         int    2       auto-scale floor
      race_duration     float  90.0    seconds
      speed             float  2.0     simulation speed multiplier
      auto_scale        bool   True
    """

    def __init__(self, config: dict):
        self.cfg = {
            "lambda_intensity": 0.009,
            "initial_nodes":    4,
            "max_nodes":        10,
            "min_nodes":        2,
            "race_duration":    90.0,
            "speed":            2.0,
            "auto_scale":       True,
            **config,
        }
        lam   = self.cfg["lambda_intensity"]
        speed = self.cfg["speed"]

        # ── URL pool + Bloom filter pre-pass ─────────────────────────────────
        raw_pool, domain_map    = generate_url_pool(total=20000, duplicate_rate=0.10)
        self._url_pool_total    = len(raw_pool)
        self._bloom             = BloomFilter(capacity=25000, fp_rate=0.01)
        filtered: list          = []

        for url in raw_pool:
            if url in self._bloom:
                self._bloom.duplicates_caught += 1
            else:
                self._bloom.add(url)
                filtered.append(url)

        self._url_pool_filtered = len(filtered)
        self._domain_map        = domain_map

        # ── Global infrastructure ─────────────────────────────────────────────
        self._lamport = LamportClock()
        self._events  = EventLog()
        self._gossip  = GossipProtocol()

        # Consistent hash ring — domain category names as real nodes
        self._ring = ConsistentHashRing(virtual_nodes=150)
        for cat in DOMAIN_CATEGORIES:
            self._ring.add_node(cat)

        # ── Raft — 3 coordinator nodes ────────────────────────────────────────
        self.raft_nodes: list = [RaftNode(i, 3) for i in range(3)]
        for rn in self.raft_nodes:
            rn.set_peers(self.raft_nodes)

        # ── The two competing systems ─────────────────────────────────────────
        self._single = SingleNodeScraper(filtered, domain_map, lam, speed)
        self._fleet  = DistributedFleet(
            url_pool      = filtered,
            domain_map    = domain_map,
            lam           = lam,
            speed         = speed,
            initial_nodes = self.cfg["initial_nodes"],
            max_nodes     = self.cfg["max_nodes"],
            min_nodes     = self.cfg["min_nodes"],
            auto_scale    = self.cfg["auto_scale"],
            global_clock  = self._lamport,
            event_log     = self._events,
            gossip        = self._gossip,
            hash_ring     = self._ring,
        )

        # Race state
        self.race_running        = False
        self.race_finished       = False
        self._start_time         = 0.0
        self._mapreduce_results  = None

        # Log boot event
        self._events.log(
            source  = "Engine",
            level   = "success",
            message = (
                f"Initialised. Pool: {self._url_pool_total} URLs → "
                f"{self._url_pool_filtered} unique after Bloom filter "
                f"({self._bloom.duplicates_caught} duplicates caught)."
            ),
            lamport = self._lamport.increment(),
            t       = 0.0,
        )

    # ── Public controls ───────────────────────────────────────────────────────

    def start_race(self) -> None:
        if self.race_running:
            return
        self._start_time  = time.time()
        self.race_running = True
        self.race_finished = False

        # Raft coordinator threads
        for rn in self.raft_nodes:
            threading.Thread(target=rn.run, daemon=True).start()

        # Both systems start simultaneously — fair race
        self._single.start(self._start_time)
        self._fleet.start(self._start_time)

        # Gossip thread
        threading.Thread(target=self._gossip_loop, daemon=True).start()

        # Race timer
        threading.Thread(target=self._race_timer, daemon=True).start()

        self._events.log(
            source  = "Engine",
            level   = "success",
            message = (
                f"Race started! λ={self.cfg['lambda_intensity']}, "
                f"nodes={self.cfg['initial_nodes']}, "
                f"duration={self.cfg['race_duration']}s, "
                f"speed={self.cfg['speed']}×"
            ),
            lamport = self._lamport.increment(),
            t       = 0.0,
        )

    def stop_race(self) -> None:
        self._end_race()

    def kill_raft_leader(self) -> str:
        """Crash the current Raft leader — watch election happen."""
        for rn in self.raft_nodes:
            if rn.state == "leader" and not rn.crashed:
                rn.force_crash()
                self._events.log(
                    source  = "Raft",
                    level   = "error",
                    message = f"Leader Coordinator-{rn.node_id} CRASHED! Election in progress...",
                    lamport = self._lamport.increment(),
                    t       = self._elapsed(),
                )
                return f"💥 Coordinator-{rn.node_id} (leader) crashed!"
        return "No live leader to crash."

    def recover_raft_node(self, node_id: int) -> None:
        for rn in self.raft_nodes:
            if rn.node_id == node_id:
                rn.recover()
                self._events.log(
                    source  = "Raft",
                    level   = "success",
                    message = f"Coordinator-{node_id} recovered and rejoined cluster.",
                    lamport = self._lamport.increment(),
                    t       = self._elapsed(),
                )

    # ── Internal ─────────────────────────────────────────────────────────────

    def _elapsed(self) -> float:
        return time.time() - self._start_time

    def _race_timer(self) -> None:
        duration = self.cfg["race_duration"]
        while self.race_running and self._elapsed() < duration:
            time.sleep(0.5)
        if self.race_running:
            self._end_race()

    def _end_race(self) -> None:
        self.race_running  = False
        self.race_finished = True

        self._single.stop()
        self._fleet.stop()
        for rn in self.raft_nodes:
            rn.stop()

        # Run MapReduce on collected distributed results
        self._mapreduce_results = run_mapreduce(self._fleet.get_results())

        dist_total = sum(w.urls_scraped for w in self._fleet._workers.values())
        self._events.log(
            source  = "Engine",
            level   = "success",
            message = (
                f"Race finished! "
                f"Single={self._single.urls_scraped} URLs | "
                f"Distributed={dist_total} URLs | "
                f"Speedup={dist_total / max(self._single.urls_scraped, 1):.1f}× | "
                f"MapReduce: {len(self._mapreduce_results)} categories."
            ),
            lamport = self._lamport.increment(),
            t       = self._elapsed(),
        )

    def _gossip_loop(self) -> None:
        """Gossip round every 2 seconds — updates node views across the cluster."""
        while self.race_running:
            node_ids = list(self._fleet._workers.keys())
            if node_ids:
                self._gossip.gossip_round(node_ids)
                for nid, w in self._fleet._workers.items():
                    self._gossip.update_node(nid, {
                        "status":  w.status,
                        "urls":    w.urls_scraped,
                        "lamport": self._lamport.value,
                    })
            time.sleep(2.0)

    # ── State snapshot  (called by dashboard every 0.8 s) ─────────────────────

    def get_state(self) -> dict:
        elapsed  = self._elapsed()
        duration = self.cfg["race_duration"]

        sn_snap    = self._single.snapshot(elapsed)
        fleet_snap = self._fleet.snapshot()

        # Inject MapReduce results once race ends
        if self._mapreduce_results is not None:
            fleet_snap["mapreduce_results"] = self._mapreduce_results

        # Pull vector clocks out of fleet snapshot for top-level key
        vc_data = fleet_snap.pop("vector_clocks", {})

        # Raft status
        raft_states = [rn.status() for rn in self.raft_nodes]
        leader_id   = next((r["id"] for r in raft_states if r["state"] == "leader"), None)

        # Append one log entry per snapshot so the Raft log visibly grows
        if self.race_running and leader_id is not None:
            self.raft_nodes[leader_id].append_log({
                "type":    "task_snapshot",
                "elapsed": round(elapsed, 1),
                "scraped": fleet_snap["urls_scraped"],
            })

        return {
            # ── Race meta ───────────────────────────────────────────────────
            "elapsed":           elapsed,
            "duration":          duration,
            "race_running":      self.race_running,
            "race_finished":     self.race_finished,

            # ── Competing systems ────────────────────────────────────────────
            "single":            sn_snap,
            "distributed":       fleet_snap,

            # ── Infrastructure ───────────────────────────────────────────────
            "raft":              raft_states,
            "raft_leader":       leader_id,
            "hash_ring":         self._ring.ring_snapshot(),
            "gossip":            self._gossip.state(total_expected=self.cfg["max_nodes"]),

            # ── Algorithms ───────────────────────────────────────────────────
            "bloom":             self._bloom.stats(),
            "url_pool_total":    self._url_pool_total,
            "url_pool_filtered": self._url_pool_filtered,
            "lamport":           self._lamport.value,
            "vector_clocks":     vc_data,
            "token_buckets":     self._fleet.get_token_bucket_stats(),

            # ── Shared ───────────────────────────────────────────────────────
            "work_queue_sizes":  self._fleet.get_work_queue_sizes(),
            "events":            self._events.all(),
        }
