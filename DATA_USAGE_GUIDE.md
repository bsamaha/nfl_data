# NFL Data Lake - Complete Usage Guide

This guide shows you how to explore, analyze, and extract insights from all the NFL data you've captured in your local data lake.

## 📊 Data Overview

Your lake contains **3 layers** of data:
- **Bronze**: Raw data as downloaded from upstream sources
- **Silver**: Cleaned, deduplicated data with consistent schemas
- **Gold**: Denormalized marts for common analytics (coming soon)

## 🗂️ Available Datasets

### Core Datasets (Enabled by Default)

| Dataset | Description | Key Fields | Partitions | Use Cases |
|---------|-------------|------------|------------|-----------|
| **`pbp`** | Play-by-play data | `game_id`, `play_id` | `year` | Game analysis, EPA analysis, drive charts |
| **`weekly`** | Player weekly stats | `season`, `week`, `player_id`, `team` | `season` | Fantasy analysis, player performance |
| **`schedules`** | Game schedules & results | `game_id` | `season` | Game tracking, team schedules |
| **`rosters`** | Team rosters by week | `season`, `week`, `player_id`, `team` | `season` | Player tracking, depth analysis |
| **`rosters_seasonal`** | Season-level rosters | `season`, `player_id` | `season` | Stable names for backfilling, season joins |
| **`injuries`** | Injury reports | `season`, `week`, `team`, `player_id` | `season` | Availability tracking, injury analysis |
| **`depth_charts`** | Depth chart positions | `season`, `week`, `team`, `position` | `season` | Starter analysis, position battles |
| **`snap_counts`** | Player snap counts | `season`, `week`, `team`, `player_id` | `season` | Usage analysis, workload tracking |
| **`players`** | Player master | `gsis_id` | none | Names, biographicals |
| **`ids`** | ID crosswalk | `gsis_id`, `pfr_id` | none | Identifier reconciliation |
| **`dk_bestball`** | DraftKings Best Ball rules | `section`, `id` | `section` | Scoring/lineup/tournament logic |

### Optional Datasets (Enable in `catalog/datasets.yml`)

| Dataset | Description | Key Fields | Partitions | Use Cases |
|---------|-------------|------------|------------|-----------|
| **`ngs_weekly`** | Next Gen Stats weekly | `season`, `week`, `player_id`, `stat_type` | `season`, `stat_type` | Advanced metrics, CPOE, speed analysis |
| **`pfr_weekly`** | PFR weekly stats | `season`, `week`, `player_id`, `stat_type` | `season`, `stat_type` | Supplemental stats, historical context |
| **`pfr_seasonal`** | PFR season stats | `season`, `player_id`, `stat_type` | `season`, `stat_type` | Supplemental seasonal stats |
| **`officials`** | Game officials | `game_id`, `official_id` | `season` | Officiating analysis |
| **`win_totals`** | Season win totals | `season`, `team` | `season` | Betting analysis, team expectations |
| **`scoring_lines`** | Game odds & lines | `season`, `game_id` | `season` | Betting analysis, game expectations |

## 🚀 Getting Started

### 1. Quick Data Exploration

```bash
# Check what data you have
ls -la data/silver/

# See available seasons for a dataset
ls data/silver/weekly/

# Check data quality and lineage
python -m src.cli profile --layer silver --datasets weekly
```

### 2. Basic DuckDB Queries

```bash
# Start DuckDB interactive shell
duckdb

# Or run queries directly
duckdb -c "SELECT COUNT(*) FROM read_parquet('data/silver/weekly/season=2024/*.parquet')"
```

## 📈 Common Analysis Patterns

### Player Performance Analysis

#### 1. Season Leaders (Any Stat)

```sql
-- Rushing yards leaders for 2024
SELECT 
    player_id,
    player_name,
    team,
    SUM(rushing_yards) AS total_yards,
    COUNT(*) AS games_played,
    AVG(rushing_yards) AS avg_yards_per_game
FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
WHERE rushing_yards > 0
GROUP BY 1, 2, 3
ORDER BY total_yards DESC
LIMIT 20;
```

#### 2. Player Week-by-Week Trends

```sql
-- Player performance over time
SELECT 
    week,
    player_name,
    rushing_yards,
    rushing_touchdowns,
    AVG(rushing_yards) OVER (
        PARTITION BY player_name 
        ORDER BY week 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3week_avg
FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
WHERE player_name = 'Christian McCaffrey'
ORDER BY week;
```

#### 3. Player Comparison

```sql
-- Compare two players side-by-side
WITH player_stats AS (
    SELECT 
        player_name,
        week,
        rushing_yards,
        receiving_yards,
        rushing_yards + receiving_yards AS total_yards
    FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
    WHERE player_name IN ('Christian McCaffrey', 'Tyreek Hill')
)
SELECT * FROM player_stats
ORDER BY player_name, week;
```

### Team Analysis

#### 1. Team Performance Trends

```sql
-- Team scoring trends
SELECT 
    team,
    week,
    points_for,
    points_against,
    points_for - points_against AS point_differential,
    AVG(points_for) OVER (
        PARTITION BY team 
        ORDER BY week 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3week_avg_points
FROM read_parquet('data/silver/schedules/season=2024/*.parquet')
WHERE week <= 18
ORDER BY team, week;
```

#### 2. Team vs Team Analysis

```sql
-- Head-to-head performance
SELECT 
    home_team,
    away_team,
    home_score,
    away_score,
    home_score - away_score AS home_margin,
    CASE 
        WHEN home_score > away_score THEN 'Home Win'
        WHEN away_score > home_score THEN 'Away Win'
        ELSE 'Tie'
    END AS result
FROM read_parquet('data/silver/schedules/season=2024/*.parquet')
WHERE (home_team = 'Kansas City Chiefs' OR away_team = 'Kansas City Chiefs')
ORDER BY week;
```

### Game Analysis

#### 1. Play-by-Play Deep Dive

```sql
-- Analyze a specific game
SELECT 
    quarter,
    down,
    distance,
    yardline,
    play_type,
    yards_gained,
    epa,
    wp,
    description
FROM read_parquet('data/silver/pbp/year=2024/*.parquet')
WHERE game_id = '2024_01_KC_LV'  -- Replace with actual game_id
ORDER BY quarter, play_id;
```

#### 2. Drive Analysis

```sql
-- Analyze drives in a game
WITH drive_summary AS (
    SELECT 
        drive,
        quarter,
        MIN(play_id) AS first_play,
        MAX(play_id) AS last_play,
        COUNT(*) AS plays,
        SUM(yards_gained) AS total_yards,
        SUM(epa) AS total_epa,
        MIN(yardline) AS start_yardline,
        MAX(yardline) AS end_yardline
    FROM read_parquet('data/silver/pbp/year=2024/*.parquet')
    WHERE game_id = '2024_01_KC_LV'
    GROUP BY drive, quarter
)
SELECT * FROM drive_summary
ORDER BY quarter, first_play;
```

### Injury & Availability Analysis

#### 1. Injury Impact on Performance

```sql
-- Compare player performance before/after injury
WITH injury_weeks AS (
    SELECT DISTINCT week, player_id
    FROM read_parquet('data/silver/injuries/season=2024/*.parquet')
    WHERE player_id = 'some_player_id'
),
performance AS (
    SELECT 
        w.week,
        w.player_id,
        w.rushing_yards,
        w.receiving_yards,
        CASE WHEN i.week IS NOT NULL THEN 'Injured' ELSE 'Healthy' END AS status
    FROM read_parquet('data/silver/weekly/season=2024/*.parquet') w
    LEFT JOIN injury_weeks i ON w.week = i.week AND w.player_id = i.player_id
    WHERE w.player_id = 'some_player_id'
)
SELECT 
    status,
    COUNT(*) AS games,
    AVG(rushing_yards) AS avg_rush,
    AVG(receiving_yards) AS avg_rec
FROM performance
GROUP BY status;
```

#### 2. Team Injury Depth

```sql
-- Team injury depth by position
SELECT 
    team,
    position,
    COUNT(DISTINCT player_id) AS total_players,
    COUNT(DISTINCT CASE WHEN week = 18 THEN player_id END) AS week_18_players
FROM read_parquet('data/silver/rosters/season=2024/*.parquet')
GROUP BY team, position
ORDER BY team, position;
```

## 🔍 Advanced Analysis Techniques

### 1. Cross-Dataset Joins

```sql
-- Combine weekly stats with depth chart info
SELECT 
    w.player_name,
    w.team,
    w.week,
    w.rushing_yards,
    d.position,
    d.depth_chart_position
FROM read_parquet('data/silver/weekly/season=2024/*.parquet') w
JOIN read_parquet('data/silver/depth_charts/season=2024/*.parquet') d
    ON w.player_id = d.player_id 
    AND w.team = d.team 
    AND w.week = d.week
WHERE w.rushing_yards > 50
ORDER BY w.rushing_yards DESC;
```

### 2. Time-Series Analysis

```sql
-- Player performance momentum
WITH weekly_performance AS (
    SELECT 
        player_id,
        player_name,
        week,
        rushing_yards,
        LAG(rushing_yards, 1) OVER (PARTITION BY player_id ORDER BY week) AS prev_week,
        LAG(rushing_yards, 2) OVER (PARTITION BY player_id ORDER BY week) AS two_weeks_ago
    FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
    WHERE rushing_yards > 0
)
SELECT 
    player_name,
    week,
    rushing_yards,
    prev_week,
    two_weeks_ago,
    CASE 
        WHEN rushing_yards > prev_week AND prev_week > two_weeks_ago THEN 'Improving'
        WHEN rushing_yards < prev_week AND prev_week < two_weeks_ago THEN 'Declining'
        ELSE 'Mixed'
    END AS trend
FROM weekly_performance
WHERE prev_week IS NOT NULL AND two_weeks_ago IS NOT NULL
ORDER BY player_name, week;
```

### 3. Statistical Analysis

```sql
-- Z-score analysis for outlier detection
WITH player_stats AS (
    SELECT 
        player_id,
        player_name,
        AVG(rushing_yards) AS avg_yards,
        STDDEV(rushing_yards) AS std_yards
    FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
    WHERE rushing_yards > 0
    GROUP BY 1, 2
    HAVING COUNT(*) >= 5  -- Minimum games played
),
outlier_weeks AS (
    SELECT 
        w.player_id,
        w.player_name,
        w.week,
        w.rushing_yards,
        (w.rushing_yards - p.avg_yards) / p.std_yards AS z_score
    FROM read_parquet('data/silver/weekly/season=2024/*.parquet') w
    JOIN player_stats p ON w.player_id = p.player_id
    WHERE w.rushing_yards > 0
)
SELECT 
    player_name,
    week,
    rushing_yards,
    ROUND(z_score, 2) AS z_score,
    CASE 
        WHEN ABS(z_score) > 2 THEN 'Outlier'
        WHEN ABS(z_score) > 1.5 THEN 'Unusual'
        ELSE 'Normal'
    END AS performance_category
FROM outlier_weeks
WHERE ABS(z_score) > 1.5
ORDER BY ABS(z_score) DESC;
```

## 📊 Data Quality & Validation

### 1. Check Data Completeness

```sql
-- Verify data coverage by season/week
SELECT 
    season,
    COUNT(DISTINCT week) AS weeks_available,
    COUNT(DISTINCT player_id) AS unique_players,
    COUNT(*) AS total_records
FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
GROUP BY season
ORDER BY season;
```

### 2. Validate Key Constraints

```sql
-- Check for duplicate keys in silver layer
SELECT 
    season,
    week,
    player_id,
    team,
    COUNT(*) AS duplicate_count
FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
GROUP BY season, week, player_id, team
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

### 3. Data Freshness Check

```sql
-- Check when data was last updated
SELECT 
    'weekly' AS dataset,
    MAX(ingested_at) AS last_updated
FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
UNION ALL
SELECT 
    'schedules' AS dataset,
    MAX(ingested_at) AS last_updated
FROM read_parquet('data/silver/schedules/season=2024/*.parquet');
```

## 🛠️ Operational Queries

### 1. Data Lake Health Check

```bash
# Check all datasets for recent activity
python -m src.cli profile --layer silver

# Check lineage for specific dataset
cat catalog/lineage.json | jq '.weekly'
```

### 2. Storage Usage

```bash
# Check data directory sizes
du -sh data/silver/*/
du -sh data/bronze/*/

# Count files per partition
find data/silver/weekly/ -name "*.parquet" | wc -l
```

### 3. Performance Monitoring

```sql
-- Check query performance on large datasets
EXPLAIN SELECT COUNT(*) FROM read_parquet('data/silver/pbp/year=2024/*.parquet');
```

## 📚 Query Templates

### Fantasy Football Analysis

```sql
-- Weekly fantasy points calculation (basic)
SELECT 
    player_name,
    team,
    week,
    rushing_yards * 0.1 + 
    rushing_touchdowns * 6 + 
    receiving_yards * 0.1 + 
    receiving_touchdowns * 6 AS fantasy_points
FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
WHERE player_name IN ('Christian McCaffrey', 'Tyreek Hill', 'Travis Kelce')
ORDER BY player_name, week;
```

### Betting Analysis

```sql
-- Game outcome vs. spread analysis
SELECT 
    home_team,
    away_team,
    home_score,
    away_score,
    spread_close,
    CASE 
        WHEN home_score - away_score > spread_close THEN 'Cover'
        WHEN home_score - away_score < spread_close THEN 'No Cover'
        ELSE 'Push'
    END AS spread_result
FROM read_parquet('data/silver/schedules/season=2024/*.parquet') s
LEFT JOIN read_parquet('data/silver/scoring_lines/season=2024/*.parquet') l
    ON s.game_id = l.game_id
WHERE spread_close IS NOT NULL
ORDER BY week;
```

### Historical Analysis

```sql
-- Player career progression
SELECT 
    season,
    player_name,
    COUNT(*) AS games_played,
    AVG(rushing_yards) AS avg_rush_yards,
    MAX(rushing_yards) AS best_game
FROM read_parquet('data/silver/weekly/season=*/**/*.parquet')
WHERE player_name = 'Adrian Peterson'
GROUP BY season, player_name
ORDER BY season;
```

## 🚨 Troubleshooting

### Common Issues

1. **Data not found**: Check if dataset is enabled in `catalog/datasets.yml`
2. **Slow queries**: Use partition filtering (`season=2024/*.parquet`)
3. **Memory issues**: Process data in chunks or use streaming
4. **Schema changes**: Check lineage for recent updates

### Debug Commands

```bash
# Check data availability
ls -la data/silver/weekly/season=2024/

# Verify file integrity
python -c "import pyarrow.parquet as pq; print(pq.read_metadata('data/silver/weekly/season=2024/file.parquet'))"

# Check lineage
python -m src.cli profile --layer silver --datasets weekly
```

## 📈 Next Steps

1. **Enable optional datasets** in `catalog/datasets.yml` as needed
2. **Build gold layer marts** for common analytics
3. **Create dashboards** using tools like Streamlit or Plotly
4. **Set up automated reporting** with cron jobs
5. **Explore advanced analytics** like player clustering or game prediction

## 🔗 Resources

- **Plan Document**: `plan.md` - Complete architecture and implementation details
- **CLI Help**: `python -m src.cli --help` - All available commands
- **Dataset Config**: `catalog/datasets.yml` - Dataset definitions and options
- **Lineage**: `catalog/lineage.json` - Data lineage and metadata
- **Quality Reports**: `catalog/quality/` - Data quality validation results

---

*This guide covers the most common use cases. For advanced scenarios or custom analysis, refer to the plan document or explore the data interactively with DuckDB.*

## 🖥️ GUI Tools for Data Lake Navigation

While DuckDB CLI is powerful, these GUI tools make it much easier to explore, visualize, and analyze your data:

### **1. DuckDB Studio (Recommended)**
- **What**: Official web-based GUI for DuckDB
- **Best For**: SQL queries, data exploration, basic visualizations
- **Setup**: 
  ```bash
  # Install DuckDB Studio
  pip install duckdb-studio
  
  # Launch with your data directory
  duckdb-studio --data-dir data/
  ```
- **Features**: 
  - Native DuckDB support
  - Query history and favorites
  - Basic charts and tables
  - Schema browser
  - Export results

### **2. DBeaver Community Edition**
- **What**: Universal database tool with excellent DuckDB support
- **Best For**: Complex queries, schema exploration, data browsing
- **Setup**:
  ```bash
  # Download from https://dbeaver.io/download/
  # Add DuckDB connection with path: data/
  ```
- **Features**:
  - Full SQL editor with syntax highlighting
  - Schema tree navigation
  - Data grid with filtering/sorting
  - Query execution plans
  - Export to multiple formats

### **3. JupyterLab + DuckDB Extension**
- **What**: Interactive notebooks with DuckDB integration
- **Best For**: Data analysis workflows, reproducible research
- **Setup**:
  ```bash
  pip install jupyterlab jupyterlab-duckdb
  jupyter lab
  ```
- **Features**:
  - Interactive SQL cells
  - Rich output display
  - Integration with pandas/polars
  - Markdown documentation
  - Chart libraries (plotly, matplotlib)

### **4. Streamlit (Custom Dashboard)**
- **What**: Python-based web app framework
- **Best For**: Building custom dashboards and interactive tools
- **Setup**:
  ```bash
  pip install streamlit
  ```
- **Example Dashboard**:
  ```python
  # dashboard.py
  import streamlit as st
  import duckdb
  
  st.title("NFL Data Lake Explorer")
  
  # Dataset selector
  dataset = st.selectbox("Choose Dataset", ["weekly", "pbp", "schedules"])
  
  # Query builder
  query = st.text_area("SQL Query", f"SELECT * FROM read_parquet('data/silver/{dataset}/season=2024/*.parquet') LIMIT 100")
  
  if st.button("Run Query"):
      conn = duckdb.connect()
      result = conn.execute(query).fetchdf()
      st.dataframe(result)
  ```

### **5. Grafana + DuckDB Plugin**
- **What**: Professional monitoring and analytics platform
- **Best For**: Real-time dashboards, operational monitoring
- **Setup**: 
  ```bash
  # Install Grafana and DuckDB plugin
  # Configure data source pointing to your data directory
  ```
- **Features**:
  - Rich visualization library
  - Dashboard templates
  - Alerting and monitoring
  - Time-series analysis
  - Team collaboration

### **6. Apache Superset**
- **What**: Enterprise BI platform
- **Best For**: Business intelligence, complex dashboards
- **Setup**: Docker-based installation
- **Features**:
  - Drag-and-drop chart builder
  - SQL Lab for queries
  - Dashboard sharing
  - Role-based access control

## 🚀 **Quick Setup Recommendations**

### **For Beginners: DuckDB Studio**
```bash
pip install duckdb-studio
duckdb-studio --data-dir data/
```
- Opens in your browser
- Point to your `data/` directory
- Start querying immediately

### **For Power Users: DBeaver + JupyterLab**
```bash
# Install both
pip install jupyterlab jupyterlab-duckdb
# Download DBeaver from website

# Launch JupyterLab
jupyter lab

# In DBeaver, create DuckDB connection to data/ directory
```

### **For Teams: Streamlit Dashboard**
```bash
pip install streamlit pandas duckdb
streamlit run dashboard.py
```

## 📊 **GUI-Specific Query Examples**

### **DuckDB Studio - Schema Browser**
```sql
-- Use the schema browser to explore tables
-- Right-click on any table to see sample data
SELECT * FROM read_parquet('data/silver/weekly/season=2024/*.parquet') LIMIT 10;
```

### **DBeaver - Visual Query Builder**
```sql
-- Use the visual query builder for complex joins
SELECT 
    w.player_name,
    w.team,
    w.week,
    w.rushing_yards,
    d.position
FROM read_parquet('data/silver/weekly/season=2024/*.parquet') w
JOIN read_parquet('data/silver/depth_charts/season=2024/*.parquet') d
    ON w.player_id = d.player_id 
    AND w.team = d.team 
    AND w.week = d.week
WHERE w.rushing_yards > 50;
```

### **JupyterLab - Interactive Analysis**
```python
# In a Jupyter cell
import duckdb
import pandas as pd

# Connect to your data
conn = duckdb.connect()

# Query and visualize
df = conn.execute("""
    SELECT week, AVG(rushing_yards) as avg_rush
    FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
    WHERE rushing_yards > 0
    GROUP BY week
    ORDER BY week
""").fetchdf()

# Plot the results
df.plot(x='week', y='avg_rush', kind='line')
```

## 🔧 **Configuration Tips**

### **DuckDB Studio Configuration**
```bash
# Launch with specific settings
duckdb-studio \
  --data-dir data/ \
  --port 8080 \
  --host 0.0.0.0
```

### **DBeaver Connection Settings**
- **Database**: `data/` (your data directory)
- **Driver**: DuckDB
- **URL**: `jdbc:duckdb:data/`
- **Username**: (leave blank)
- **Password**: (leave blank)

### **JupyterLab DuckDB Extension**
```python
# In a notebook cell
%load_ext jupyterlab_duckdb
%duckdb data/
```

## 📈 **Visualization Examples**

### **Player Performance Dashboard**
```python
# Streamlit example
import streamlit as st
import duckdb
import plotly.express as px

st.title("NFL Player Performance Dashboard")

# Player selector
conn = duckdb.connect()
players = conn.execute("""
    SELECT DISTINCT player_name 
    FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
    ORDER BY player_name
""").fetchdf()

selected_player = st.selectbox("Select Player", players['player_name'])

# Performance chart
if selected_player:
    data = conn.execute(f"""
        SELECT week, rushing_yards, receiving_yards
        FROM read_parquet('data/silver/weekly/season=2024/*.parquet')
        WHERE player_name = '{selected_player}'
        ORDER BY week
    """).fetchdf()
    
    fig = px.line(data, x='week', y=['rushing_yards', 'receiving_yards'],
                  title=f"{selected_player} - Weekly Performance")
    st.plotly_chart(fig)
```

## 🎯 **Tool Selection Guide**

| Use Case | Best Tool | Why |
|----------|-----------|-----|
| **Quick exploration** | DuckDB Studio | Fastest setup, native DuckDB support |
| **Complex queries** | DBeaver | Best SQL editor, schema browser |
| **Data analysis** | JupyterLab | Interactive, reproducible, rich ecosystem |
| **Custom dashboards** | Streamlit | Python-based, highly customizable |
| **Team BI** | Superset | Enterprise features, collaboration |
| **Monitoring** | Grafana | Real-time, alerting, time-series |

## 🚨 **Common GUI Issues & Solutions**

### **"No tables found" Error**
- Ensure you're pointing to the correct `data/` directory
- Check that Parquet files exist in the expected structure
- Verify file permissions

### **Slow Performance**
- Use partition filtering in queries (`season=2024/*.parquet`)
- Limit result sets with `LIMIT` clauses
- Consider using DuckDB's `read_parquet` with specific file paths

### **Memory Issues**
- Process data in chunks
- Use streaming queries for large datasets
- Monitor system resources during heavy queries

---

*These GUI tools will make your data lake exploration much more intuitive. Start with DuckDB Studio for immediate results, then graduate to more powerful tools as your needs grow.*

## 🐧 **WSL2 Setup & Considerations**

All these GUI tools work great in WSL2! Here's how to set them up and access them from Windows:

### **WSL2 Network & Port Forwarding**

WSL2 runs in a virtual network, so you'll need to forward ports to access web-based tools from Windows:

```bash
# Check your WSL2 IP address
ip addr show eth0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1

# Example output: 172.22.123.45
# Your Windows browser will access: http://172.22.123.45:8080
```

### **1. DuckDB Studio in WSL2 (Recommended)**

```bash
# Install and launch
pip install duckdb-studio

# Launch with external access
duckdb-studio --data-dir data/ --host 0.0.0.0 --port 8080

# Access from Windows browser:
# http://172.22.123.45:8080 (replace with your WSL2 IP)
```

**WSL2 Benefits**: 
- Native Linux performance
- Direct access to your data directory
- No file path translation issues

### **2. DBeaver in WSL2**

**Option A: Install in WSL2 (Recommended)**
```bash
# Install DBeaver in WSL2
wget -O - https://dbeaver.io/debs/dbeaver.gpg.key | sudo apt-key add -
echo "deb https://dbeaver.io/debs/dbeaver-ce /" | sudo tee /etc/apt/sources.list.d/dbeaver.list
sudo apt update
sudo apt install dbeaver-ce

# Launch DBeaver
dbeaver
```

**Option B: Install in Windows, Connect to WSL2**
- Install DBeaver in Windows
- Connect to WSL2 using SSH or file path mapping
- Database path: `\\wsl$\Ubuntu\home\r16\workspace\nfl_data\data\`

**WSL2 Benefits**: 
- Direct file system access
- Better performance with large datasets
- Native Linux environment

### **3. JupyterLab in WSL2**

```bash
# Install and launch
pip install jupyterlab jupyterlab-duckdb

# Launch with external access
jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root

# Access from Windows browser:
# http://172.22.123.45:8888
```

**WSL2 Benefits**:
- Full Python ecosystem
- Direct access to system packages
- Better memory management for large datasets

### **4. Streamlit in WSL2**

```bash
# Install and launch
pip install streamlit

# Launch with external access
streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0

# Access from Windows browser:
# http://172.22.123.45:8501
```

**WSL2 Benefits**:
- Native Python performance
- Direct file system access
- Easy deployment to production

### **5. Grafana in WSL2**

```bash
# Install Grafana
sudo apt update
sudo apt install grafana

# Start Grafana service
sudo systemctl start grafana-server
sudo systemctl enable grafana-server

# Access from Windows browser:
# http://172.22.123.45:3000
# Default credentials: admin/admin
```

**WSL2 Benefits**:
- System service management
- Persistent across reboots
- Native Linux performance

## 🔧 **WSL2-Specific Configuration**

### **Port Forwarding Script**

Create a script to automatically forward ports:

```bash
# ~/forward-ports.sh
#!/bin/bash

# Get WSL2 IP
WSL_IP=$(ip addr show eth0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)

echo "WSL2 IP: $WSL_IP"
echo "Forwarding ports to Windows..."

# Forward common ports
netsh interface portproxy add v4tov4 listenport=8080 listenaddress=0.0.0.0 connectport=8080 connectaddress=$WSL_IP
netsh interface portproxy add v4tov4 listenport=8888 listenaddress=0.0.0.0 connectport=8888 connectaddress=$WSL_IP
netsh interface portproxy add v4tov4 listenport=8501 listenaddress=0.0.0.0 connectport=8501 connectaddress=$WSL_IP

echo "Ports forwarded! Access from Windows:"
echo "DuckDB Studio: http://localhost:8080"
echo "JupyterLab: http://localhost:8888"
echo "Streamlit: http://localhost:8501"
```

**Note**: Run this script from Windows PowerShell as Administrator.

### **Windows Firewall Rules**

```powershell
# Run in Windows PowerShell as Administrator
New-NetFirewallRule -DisplayName "WSL2 Data Tools" -Direction Inbound -Protocol TCP -LocalPort 8080,8888,8501 -Action Allow
```

### **WSL2 Memory & Performance Tuning**

```bash
# ~/.wslconfig (create in Windows user directory)
[wsl2]
memory=8GB
processors=4
swap=2GB
localhostForwarding=true
```

## 🚀 **WSL2 Quick Start Commands**

### **Launch All Tools at Once**

```bash
# Create a launch script
cat > ~/launch-data-tools.sh << 'EOF'
#!/bin/bash

# Get WSL2 IP
WSL_IP=$(ip addr show eth0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1)
echo "WSL2 IP: $WSL_IP"

# Launch DuckDB Studio
echo "Launching DuckDB Studio..."
duckdb-studio --data-dir data/ --host 0.0.0.0 --port 8080 &
STUDIO_PID=$!

# Launch JupyterLab
echo "Launching JupyterLab..."
jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root &
JUPYTER_PID=$!

# Launch Streamlit (if dashboard exists)
if [ -f "dashboard.py" ]; then
    echo "Launching Streamlit..."
    streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0 &
    STREAMLIT_PID=$!
fi

echo ""
echo "🎉 Data tools launched!"
echo "DuckDB Studio: http://$WSL_IP:8080"
echo "JupyterLab: http://$WSL_IP:8888"
if [ -f "dashboard.py" ]; then
    echo "Streamlit: http://$WSL_IP:8501"
fi
echo ""
echo "Press Ctrl+C to stop all tools"

# Wait for interrupt
trap "kill $STUDIO_PID $JUPYTER_PID $STREAMLIT_PID 2>/dev/null; exit" INT
wait
EOF

chmod +x ~/launch-data-tools.sh

# Run the script
~/launch-data-tools.sh
```

### **Access URLs from Windows**

Once launched, access from Windows browser:
- **DuckDB Studio**: `http://localhost:8080` (after port forwarding) or `http://172.22.123.45:8080`
- **JupyterLab**: `http://localhost:8888` or `http://172.22.123.45:8888`
- **Streamlit**: `http://localhost:8501` or `http://172.22.123.45:8501`

## 🎯 **WSL2 Tool Selection for Your Setup**

| Tool | WSL2 Setup | Windows Access | Best For |
|------|------------|----------------|----------|
| **DuckDB Studio** | ✅ Native | ✅ Browser | Quick exploration |
| **DBeaver** | ✅ Native | ✅ Direct | Complex queries |
| **JupyterLab** | ✅ Native | ✅ Browser | Analysis workflows |
| **Streamlit** | ✅ Native | ✅ Browser | Custom dashboards |
| **Grafana** | ✅ Native | ✅ Browser | Monitoring |
| **Superset** | ✅ Docker | ✅ Browser | Enterprise BI |

## 🚨 **WSL2-Specific Issues & Solutions**

### **"Address Already in Use" Error**
```bash
# Check what's using the port
sudo netstat -tlnp | grep :8080

# Kill the process
sudo kill -9 <PID>

# Or use a different port
duckdb-studio --port 8081
```

### **Can't Access from Windows Browser**
```bash
# Check WSL2 IP
ip addr show eth0

# Check if service is listening on 0.0.0.0
netstat -tlnp | grep :8080

# Test locally in WSL2
curl http://localhost:8080
```

### **File Permission Issues**
```bash
# Fix data directory permissions
chmod -R 755 data/
chown -R $USER:$USER data/
```

### **Memory Issues with Large Datasets**
```bash
# Check WSL2 memory usage
free -h

# Adjust WSL2 memory in ~/.wslconfig
# memory=16GB  # Increase if needed
```

## 💡 **WSL2 Pro Tips**

1. **Use Port Forwarding**: Set up port forwarding for seamless Windows access
2. **Monitor Resources**: Keep an eye on WSL2 memory usage with large datasets
3. **File Paths**: Use relative paths (`data/`) instead of absolute paths
4. **Service Management**: Use systemd for persistent services like Grafana
5. **Backup**: Your data is in the Windows file system, so it's automatically backed up

---

*WSL2 gives you the best of both worlds: Linux performance for data processing and Windows integration for GUI access. All these tools work seamlessly in your WSL2 environment!*
