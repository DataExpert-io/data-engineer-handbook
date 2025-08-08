# 📊 Get Set for Data Modeling (Weeks 1 & 2)

Welcome, Data Explorer! 🚀 Whether you're just beginning or brushing up on skills, this guide will walk you through everything you need — from installing tools to troubleshooting hiccups — all in one friendly place.

---

## 🧰 Your Dev Toolkit at a Glance

🟦 **Git**  
_Clone code, version your work._

🟪 **PostgreSQL**  
_Reliable, powerful open-source database engine._

⬛ **PSQL (CLI)**  
_Talk directly to your database via terminal._

🐳 **Docker + Compose**  
_Spin up Postgres + PGAdmin instantly, no manual setup._

🧑‍💻 **PGAdmin / DBeaver / VS Code**  
_Graphical tools for exploring and querying your data._

---

## 📝 Your Setup in 3 Steps

### Step 1️⃣: Download the Code

Clone the course files onto your machine:

```bash
git clone git@github.com:DataExpert-io/data-engineer-handbook.git
cd data-engineer-handbook/bootcamp/materials/1-dimensional-data-modeling
```

> 🔐 Need SSH set up first? Use [GitHub’s SSH guide](https://docs.github.com/en/authentication/connecting-to-github-with-ssh)

---

### Step 2️⃣: Start PostgreSQL

#### 🐳 Option A: Docker (Simplest & Preferred)

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop)  
2. Copy the env template:

```bash
cp example.env .env
```

> The `.env` file stores credentials used by PostgreSQL and PGAdmin

3. Start PostgreSQL & PGAdmin in containers:

```bash
# Mac users:
make up

# Windows (or general):
docker compose up -d
```

4. Check containers are running:

```bash
docker ps -a
```

5. When you're done with work:

```bash
docker compose stop
```

---

#### 🧩 Option B: Local Installation (Manual Setup)

1. Install PostgreSQL  
   - [Mac – use Homebrew](https://brew.sh/)  
   - [Windows – official installer](https://www.postgresql.org/download/)

2. Restore the sample database:

```bash
pg_restore -c --if-exists -U <your-username> -d postgres data.dump
```

If that fails, try:

```bash
pg_restore -U [username] -d [db_name] -h [host] -p [port] data.dump
```

---

### Step 3️⃣: Connect to PostgreSQL

Choose any GUI tool you like. Here’s how:

#### 🌐 If using PGAdmin (via Docker browser)

1. Go to [http://localhost:5050](http://localhost:5050)  
2. Log in using the credentials from your `.env` file  
3. Create a new server:  
	1. `Dashboard` ➜ `Quick Links` ➜ `Add New Server`
	2. Under the `General` tab: give it a friendly `Name`, e.g. `Data-Engineer-Handbook-DB`
	3. `Connection` tab: Copy in credentials from `.env`, where the defaults are:
	   - **Name**: Name of your choice  
	   - **Host**: `my-postgres-container`  
	   - **Port**: `5432`  
	   - **Database**: `postgres`  
	   - **Username**: `postgres`  
	   - **Password**: `postgres`  
	   - ✅ Save Password  
4. Click **Save** — and you’re connected!
5. Expand `Servers`  › *`your-server`* › `Databases` › `postgres`
	- The database must be highlighted to be able to open the `Query Tool`
	- Further expanding `postgres` › `Schemas` › `public` › `Tables` should show the expected content

---

#### 💻 If using a desktop client (like DataGrip, DBeaver, or VS Code)

Use the following values to set up a new PostgreSQL connection:
   - **Host**: `localhost`  
   - **Port**: `5432`  
   - **Database**: `postgres`  
   - **Username**: `postgres`  
   - **Password**: `postgres`  
   - ✅ Save Password  

✅ Test & Save your connection and you’re good to go.

---

## 🧩 Tables Not Loading? Let’s Fix It!

If you don’t see any tables after restoring the database, try these steps depending on how you installed Postgres:

### 📦 For Local Installation (No Docker)

1. **Find your `psql` client executable** (on Windows):

```bash
C:\Program Files\PostgreSQL\13\runpsql.bat
```

Or search for **SQL Shell (psql)** in your Start menu.

2. **Open your terminal and `cd` into the repo folder**, where `data.dump` is located.

3. **Run `psql` and enter credentials** (username is usually `postgres`)

4. Once you’re inside the Postgres prompt (`postgres=#`), run:

```sql
\i data.dump
```

> 🧠 This tells Postgres to execute all SQL commands inside the dump file, creating tables and loading data.

---

### 🐳 For Docker Users

1. Get your running containers:

```bash
docker ps
```

2. Copy the name of your Postgres container (e.g., `my-postgres-container`)

3. Open a bash terminal inside it:

```bash
docker exec -it my-postgres-container bash
```

4. Run the restore manually from inside the container:

```bash
pg_restore -U $POSTGRES_USER -d $POSTGRES_DB /docker-entrypoint-initdb.d/data.dump
```

> ✅ Replace `$POSTGRES_USER` and `$POSTGRES_DB` with actual values from your `.env` file if needed.

5. Optionally check if tables are loaded:

```bash
psql -U postgres -d postgres -c '\dt'
```

This shows all the tables in the current schema.

---

## ❓ Common Errors & Fixes

### ❌ “Connection refused” or can’t connect to localhost?

- Double check host is correct (`localhost` or `my-postgres-container`)
- Ensure Docker is running and the container is up
- Try restarting the services with `make restart`

---

### 🔄 Port 5432 already in use?

You may have another service (like another DB) using it.

#### macOS:

```bash
lsof -i :5432
kill -9 <PID>
```

#### Windows:

```cmd
netstat -ano | findstr :5432
taskkill /PID <PID> /F
```

---

### 🚪 PGAdmin login not working?

Make sure you’re using values from `.env`:

```env
PGADMIN_DEFAULT_EMAIL=postgres@postgres.com
PGADMIN_DEFAULT_PASSWORD=postgres
```

If you've changed the `.env`, delete the PGAdmin container and re-run `make up`.

---

### 🕵️ Not sure which container is which?

Run:

```bash
docker ps
```

Look under the `NAMES` column for `my-postgres-container` and `pgadmin`.

---

### 🔁 Want a fresh start?

Stop and remove all running containers:

```bash
docker compose down
docker compose up -d
```

Or use:

```bash
make restart
```

---

## 🔧 Helpful Docker Make Commands

| Command           | What it does                    |
|------------------|----------------------------------|
| `make up`        | Start Postgres and PGAdmin       |
| `make stop`      | Stop both containers             |
| `make restart`   | Restart the Postgres container   |
| `make logs`      | View logs from containers        |
| `make inspect`   | Inspect container configuration  |
| `make ip`        | Get container IP address         |

---

🎉 That’s it! You’re all set for the next chapters of your data journey.
