# ScrapeGrid

A visual simulator that races one scraper against a distributed fleet and shows why scale needs coordination.

## What Is This?

ScrapeGrid lets you run a live scraping race in your browser. One side is a single worker. The other side is a group of workers that share work, recover from blocks, and coordinate as a small distributed system.

Scraping at scale gets hard fast. Sites block repeated requests, workers fail, queues become uneven, and duplicate URLs waste time. Real companies like Google, Cloudflare, and Amazon solve these problems with coordination tools such as rate limiting, consensus, load balancing, and fault recovery.

ScrapeGrid simulates all of that in your browser.

## Live Demo / Screenshots

Add 3-4 screenshots here: Race tab, Nodes tab, Architecture tab, Analytics tab.

Add images in `/assets/screenshots/` and link them here.

Example:

```md
![Race tab](assets/screenshots/race.png)
![Nodes tab](assets/screenshots/nodes.png)
![Architecture tab](assets/screenshots/architecture.png)
![Analytics tab](assets/screenshots/analytics.png)
```

## What You Will See When You Run It

The **Race** tab shows the main comparison. You can watch one scraper compete against a distributed fleet. It shows URLs scraped, IP blocks, throughput, dead time, and scale events.

The **Nodes** tab shows each worker. You can see whether a worker is scraping, blocked, cooling down, idle, or done. It also shows work stealing, which means idle workers take tasks from busier queues.

The **Architecture** tab shows the system design while it runs. You can crash the Raft leader and watch another coordinator take over. Raft is a leader election algorithm, which means the system chooses one coordinator to make decisions.

The **Algorithms** tab turns the invisible parts into charts. You can inspect the Bloom filter, token buckets, Lamport clock, and vector clocks. These are the small tools that make the larger system behave.

The **Event Log** tab shows what happened in order. Events are sorted with a Lamport clock, which is a counter used to order events across different workers.

The **Analytics** tab appears after a race. It compares final performance, shows each node's contribution, and runs a MapReduce summary. MapReduce means "split work into pieces, group the results, then combine them."

## The Algorithms Inside

**Bloom Filter**  
Quickly checks whether a URL was probably seen before.  
It matters because large crawlers need to avoid wasting time on duplicates.

**Consistent Hashing**  
Assigns work to shards so only a small part moves when nodes change.  
Companies use this idea in caches, databases, and load balancers.

**Circuit Breaker**  
Stops a worker from retrying immediately after it gets blocked.  
This protects systems from making a bad failure worse.

**Token Bucket Rate Limiter**  
Controls how many requests each domain can receive per second.  
It is used to prevent overload and respect request limits.

**Lamport Clock**  
Gives events a logical order across workers.  
It helps explain what happened first when machines do not share one clock.

**Vector Clocks**  
Track which worker knows about which events.  
They help detect true concurrency, where two things happen independently.

**Raft Consensus**  
Elects a leader among coordinator nodes.  
Distributed systems use consensus when they need agreement after failures.

**Gossip Protocol**  
Spreads node state through random peer-to-peer exchanges.  
It is useful when a central health checker would be too fragile.

**Work Stealing**  
Lets idle workers take tasks from busy workers.  
This keeps the fleet productive when work is uneven.

**Auto-Scaling**  
Adds or removes workers based on load and block rate.  
Cloud platforms use this to match capacity with demand.

**MapReduce**  
Groups scraped results and computes summary statistics.  
It is a classic pattern for processing large datasets in parallel.

## Project Structure

```text
DISTRIBUTED_SCRAPER_PDC/
├── algorithms/
│   ├── bloom_filter.py        # Remembers seen URLs using little memory.
│   ├── circuit_breaker.py     # Pauses blocked workers before retrying.
│   ├── clocks.py              # Orders events across different workers.
│   ├── consistent_hash.py     # Assigns work to stable shards.
│   ├── gossip.py              # Shares worker state without one central checker.
│   ├── raft.py                # Elects a coordinator leader.
│   └── token_bucket.py        # Limits request speed per domain.
├── tests/                     # Unit tests for the algorithm modules.
├── utils/
│   ├── logger.py              # Stores recent simulation events.
│   └── url_generator.py       # Creates fake URLs and fake page content.
├── dashboard.py               # The Streamlit app you run in the browser.
├── simulation.py              # The brain of the simulation.
├── requirements.txt           # Runtime Python packages.
└── requirements-dev.txt       # Testing and linting packages.
```

## How To Run It

1. Clone the repo.

```bash
git clone https://github.com/your-username/ScrapeGrid.git
cd ScrapeGrid
```

2. Create a virtual environment.

```bash
python -m venv venv
```

3. Activate it.

Windows:

```powershell
venv\Scripts\activate
```

Mac/Linux:

```bash
source venv/bin/activate
```

4. Install dependencies.

```bash
pip install -r requirements.txt
```

5. Start the dashboard.

```bash
streamlit run dashboard.py
```

It will open automatically in your browser at `localhost:8501`.

## Configuration

| Setting | What it does | Default |
| --- | --- | --- |
| IP Block Intensity | Controls how quickly repeated requests trigger simulated blocks. Higher means workers get blocked faster. | `0.009` |
| Initial Worker Nodes | Sets how many distributed workers start the race. | `4` |
| Max Nodes | Sets the highest number of workers auto-scaling can create. | `10` |
| Race Duration | Sets how long the simulation runs. | `90 seconds` |
| Simulation Speed | Speeds up or slows down simulated scraping delays. | `2.0x` |
| Auto-scaling enabled | Allows the fleet to add or remove workers based on load. | `On` |
| Start Race | Starts a new simulation with the selected settings. | Button |
| Stop | Stops the current race early. | Button |
| Reset | Clears the current engine so you can start fresh. | Button |
| Crash Raft Leader | Simulates coordinator failure so you can watch re-election. | Button |
| Recover Node | Brings a crashed Raft coordinator back into the cluster. | Button |

## Tech Stack

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-red)
![Plotly](https://img.shields.io/badge/Plotly-charts-3f4f75)
![Pandas](https://img.shields.io/badge/Pandas-tables-150458)
![NumPy](https://img.shields.io/badge/NumPy-math-013243)

Python runs the simulator, Streamlit builds the browser dashboard, Plotly draws charts, Pandas formats tables, and NumPy helps with chart geometry.

## Why I Built This

I wanted to go beyond slides and actually see these algorithms running. Most Parallel and Distributed Computing courses show you theory. ScrapeGrid shows you theory moving.

I also wanted a project that felt close to real engineering problems. Scraping, rate limits, failures, queues, and scaling are things production teams actually deal with.

Building it helped me connect textbook ideas to systems I can explain, test, and improve.

## What I Learned / What's Next

- Distributed systems are mostly about handling messy situations, not just making things faster.
- A single worker is simple, but it becomes fragile when blocks and delays appear.
- Rate limiting matters because speed without control can damage the system.
- Leader election is easier to understand when you can crash the leader yourself.
- Logical clocks make event ordering visible when real time is not enough.
- Good dashboards make algorithms easier to explain.

Future extensions:

- Use real HTTP requests with robots.txt and retry policies.
- Run workers as actual distributed processes with Docker.
- Add persistent storage for scraped results and event history.

## License

MIT License.

## Author

Saqib Mahmood - Khubaib Durrani 
